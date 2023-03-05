from decimal import Decimal
from math import ceil
from app.common.models.client import Client
from app.common.models.product import Product
from app.common.util import agg_concat, periodAdjust, quarter2date
from app.gp.models.price_type.bucket import AggBucket, FormulaOutput, RatioResults
from app.gp.models.price_type.enums import PriceTypeEnum
from app.gp.models.price_type.price_type import Formula, GPPriceType, LogicBucket
from app.gp.models.run import PriceTypeExecution
from sqlalchemy import func, union_all, or_, and_, case, text, literal
from sqlalchemy.sql import select
from app import db
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import aliased
from app.gp.services import output_util
from app.gp.services.price_type.price_type import (
    PRIOR_PERIOD_OFFSETS,
    PRICE_TYPE_BLEND_COLS,
    filter_on_ratio,
    get_run_domain_query,
    effective_blend_join,
    PRICE_TYPE_ELIG,
    get_blend_columns,

)

def get_agg_bucket_report_query(
    client_id,
    price_type,
    exec_id,
    periods,
    group_by_period=None,
    ignore_method=None,
    method_period=None
):
    execution = PriceTypeExecution.query.get(exec_id)
    run_price_type = execution.run_price_type

    product_query = get_run_domain_query(client_id, Product, run_price_type)

    # First limit products based on eligibility for price type
    price_type_enum = price_type.price_type
    if price_type_enum in PRICE_TYPE_ELIG:
        product_query = product_query.filter(
            PRICE_TYPE_ELIG[price_type.price_type].is_(True)
        )

    # Join to ProductBlend and establish blending relationships (if any)
    product_query = effective_blend_join(client_id, product_query, run_price_type)

    # Determining earliest effective_date of blend if requested and applicable
    # This is required during formula calculation
    prod_effective_col = Product.effective_date
   
    ndc11_blend, ndc9_blend = get_blend_columns(price_type, nat_nine=True)

    def _calc_year(period):
        if price_type_enum in (PriceTypeEnum.TEMPNFAMP, PriceTypeEnum.PERMNFAMP):
            # Only one true period exists for each product, so the value being used
            # as period component in group_by doesn't really matter
            year = max(periods)
        else:
            year = str(int(period[:4]) + 1) if period[4:] > "09" else period[:4]

        return year

    periods = [
        (
            _calc_year(period),
            period[:4] + "Q" + str(ceil(int(period[4:]) / 3)),
            period,
        )
        for period in periods
    ]

    # If calculation method (5i/RCP) is to be ignored, need to ensure buckets for
    # both versions are returned (even if zero)
    if ignore_method and price_type_enum in (
        PriceTypeEnum.AMP,
        PriceTypeEnum.NFAMP,
        PriceTypeEnum.ANFAMP,
        PriceTypeEnum.TEMPNFAMP,
        PriceTypeEnum.PERMNFAMP,
    ):
        methods = (True, False)
    else:
        methods = (False,)

    # Period start/end converted to quarterly values as products
    # need to be around through the expiration quarter
    # - Monthly AMP only delivered to pricing table starting FSD month through LLE month
    periods_table = union_all(
        *[
            select(
                [
                    literal(ratio1).cast(db.Boolean).label("ratio1"),
                    literal(period[2]).cast(db.String).label("period"),
                    literal(period[1]).cast(db.String).label("quarter"),
                    literal(period[0]).cast(db.String).label("year"),
                    literal(quarter2date(period[1])).label("period_start"),
                    literal(period[2][:4]+ '-' +period[2][4:]+ '-'+'01').label("date"),
                    literal(period[2][:4]+ '-' +period[2][4:]).label("month"),
                    literal(quarter2date(period[1], is_last=True)).label("period_end"),
                ]
            )
            for period in periods
            for ratio1 in methods
        ]
    ).cte()

    base_query = product_query.join(
        LogicBucket,
        and_(
            LogicBucket.price_type_id == price_type.id,
            LogicBucket.derived == False,  # noqa
            LogicBucket.enabled == True,  # noqa
        ),
    )

    base_query = base_query.join(
        GPPriceType, GPPriceType.id == price_type.id,
    ).join(
        Client, Client.id == GPPriceType.client_id
    )

    if price_type.price_type == PriceTypeEnum.DATA_SUMMARY:
        base_query = base_query.join(periods_table, text("1=1"))
    else:
        # blend_start will be used in place of the product's effective date if applicable
        base_query = base_query.join(
            periods_table,
            and_(
                periods_table.c.period_start <= Product.expiration_date,
                periods_table.c.period_end >= prod_effective_col,
            ),
        )

    base_query = base_query.with_entities(
        LogicBucket.id.label("logic_id"),
        LogicBucket.name,
        LogicBucket.reportable_name,
        LogicBucket.source,
        LogicBucket.unbucketed,
        LogicBucket.ordering,
        LogicBucket.value_period,
        LogicBucket.summary,
        LogicBucket.summary_variance,
        LogicBucket.variance_threshold_dollar,
        LogicBucket.variance_threshold_unit,
        Product.ndc11,
        ndc11_blend.label("ndc11_blend"),
        ndc9_blend.label("ndc9"),
        "period",
        "quarter",
        "year",
        "ratio1",
        "date",
        "month",
        GPPriceType.id.label("price_type_id"),
        GPPriceType.name.label("price_type_name"),
        GPPriceType.price_type.label("price_type"),
        Client.name.label("client")
    )

    # addl_filters = []
    # if bucket_type == "summary":
    #     addl_filters.append(LogicBucket.summary.is_(True))
    # elif bucket_type == "summary_variance":
    #     addl_filters.append(LogicBucket.summary_variance.is_(True))
    # elif bucket_type == "reportable":
    #     addl_filters.append(LogicBucket.reportable.is_(True))
    # elif bucket_type == "adhoc":
    #     addl_filters.append(LogicBucket.name.in_(adhoc_buckets))
    # elif bucket_type == "unbucketed":
    #     addl_filters += [
    #         LogicBucket.unbucketed.is_(True),
    #         LogicBucket.calc_group != CalcGroupEnum.PRE,
    #     ]

    # base_query = base_query.filter(*addl_filters)
    base_query = base_query.subquery()

    bucket_query = (
        AggBucket.query.join(LogicBucket, AggBucket.logic_id == LogicBucket.id)
        .join(
            periods_table,
            and_(
                periods_table.c.period == AggBucket.period,
                case(
                    [
                        (
                            literal(ignore_method),
                            periods_table.c.ratio1 == AggBucket.ratio1,
                        )
                    ],
                    else_=literal(True),
                ),
            ),
        )
        .filter(
            LogicBucket.price_type_id == price_type.id, AggBucket.exec_id == exec_id
        )
    )

    if not ignore_method:
        bucket_query = filter_on_ratio(
            bucket_query, AggBucket, price_type, exec_id, method_period, group_by_period
        )

    
    if group_by_period:
        if group_by_period == "quarterly":
            period_col = periods_table.c.quarter
            period_filter = func.substr(AggBucket.period, 5).in_(
                ["03", "06", "09", "12"]
            )
        elif group_by_period == "yearly":
            period_col = periods_table.c.year
            if price_type_enum in (PriceTypeEnum.TEMPNFAMP, PriceTypeEnum.PERMNFAMP):
                max_product_period = (
                    bucket_query.group_by(AggBucket.ndc11)
                    .with_entities(
                        AggBucket.ndc11, func.max(AggBucket.period).label("max_period")
                    )
                    .subquery()
                )

                bucket_query = bucket_query.join(
                    max_product_period, max_product_period.c.ndc11 == AggBucket.ndc11
                )
                period_filter = max_product_period.c.max_period == AggBucket.period
            else:
                period_filter = func.substr(AggBucket.period, 5) == "09"

        bucket_query = (
            bucket_query.group_by(
                LogicBucket.derived,
                AggBucket.ndc11,
                period_col,
                AggBucket.ratio1,
                AggBucket.logic_name,
                AggBucket.logic_value,
            ).with_entities(
                func.sum(
                    case(
                        [
                            (
                                period_filter,
                                AggBucket.id,
                            )
                        ],
                        else_=literal(0),
                    )
                ).label(
                    "id"
                ),  
                agg_concat(AggBucket.id.cast(db.String), ",").label("ids"),
                LogicBucket.derived,
                AggBucket.ndc11,
                func.max(AggBucket.period).label("period"),
                period_col,
                AggBucket.ratio1,
                AggBucket.logic_name,
                AggBucket.logic_value,
                func.sum(AggBucket.dollars).label("dollars"),
                func.sum(AggBucket.units).label("units"),
                func.min(AggBucket.min_value).label("min_value"),
                func.sum(AggBucket.count).label("count"),
            )
        ).subquery()

        if group_by_period == "quarterly":
            period_join = base_query.c.quarter == bucket_query.c.quarter
        elif group_by_period == "yearly":
            period_join = base_query.c.year == bucket_query.c.year

    else:
        bucket_query = bucket_query.with_entities(
            LogicBucket.derived,
            AggBucket,
            periods_table.c.quarter,
            periods_table.c.year,
            AggBucket.id.cast(db.String).label("ids"),
        ).subquery()

        period_join = base_query.c.period == bucket_query.c.period

    bucket_ratio = False
    amp_ratio = False
    
    if price_type.price_type == PriceTypeEnum.AMP:
        amp_ratio = True

    if price_type.price_type in (
        PriceTypeEnum.AMP,
        PriceTypeEnum.NFAMP,
        PriceTypeEnum.ANFAMP,
        PriceTypeEnum.TEMPNFAMP,
        PriceTypeEnum.PERMNFAMP,
    ):
        bucket_ratio = True

    final_query = (
        select(
            [
                base_query.c.logic_id.label("id"),
                base_query.c.source,
                base_query.c.name.label("bucket"),
                base_query.c.ordering.label("order"),
                base_query.c.ndc11,
                base_query.c.ndc9,
                base_query.c.period,
                base_query.c.price_type_id.label("calc_id"),
                base_query.c.price_type_name.label("calc"),
                base_query.c.client,
                base_query.c.year,
                base_query.c.quarter,
                base_query.c.month,
                base_query.c.date,
                func.coalesce(bucket_query.c.dollars, Decimal("0.00")).label("amt"),
                func.coalesce(bucket_query.c.units, Decimal("0.000")).label("units"),
                # func.coalesce(bucket_query.c.count, 0).label("count"),
                case(
                    [
                       (literal(bucket_ratio),
                        case(
                            [
                                (literal(amp_ratio),
                                case(
                                    [
                                      (bucket_query.c.ratio1.is_(True), "5i")
                                    ],
                                    else_="RCP",
                                )
                                )
                            ],
                            else_=case(
                                [
                                    (bucket_query.c.ratio1.is_(True), "DS")
                                ],
                                else_="WHL",
                            )
                        )
                       )
                    ],
                    else_="",
                ).label("method"),
            ]
        )
        .select_from(
            base_query.outerjoin(
                bucket_query,
                and_(
                    base_query.c.name == bucket_query.c.logic_name,
                    base_query.c.ndc11_blend == bucket_query.c.ndc11,
                    period_join,
                    case(
                        [
                            (
                                literal(ignore_method),
                                base_query.c.ratio1 == bucket_query.c.ratio1,
                            )
                        ],
                        else_=literal(True),
                    ),
                ),
            )
        )
        .where(or_(base_query.c.unbucketed == False, bucket_query.c.count != 0))
    )

    final_query = final_query.order_by(
        base_query.c.name, base_query.c.ndc11, base_query.c.period
    )

    return final_query


def get_agg_bucket_report(
    client_id,
    price_type,
    exec_id,
    periods,
    period_type,
    ignore_method=False,
    method_period=None
):
    query = get_agg_bucket_report_query(
            client_id,
            price_type,
            exec_id,
            periods,
            group_by_period=period_type,
            ignore_method=ignore_method,
            method_period=method_period
        )
    # print("agg_bucket_query.....", query)
    execution = PriceTypeExecution.query.get(exec_id)
    run_price_type = execution.run_price_type
    product_period_filter = output_util.filter_on_product_period(
        price_type.client_id, run_price_type, price_type
    )

    # max_period = max(periods)
    # notes = get_notes(exec_id, AggBucket, "bucket")
    agg_buckets = []

    def _format_bucket(agg_bucket):
        # Reformats agg_bucket for json
        agg_bucket = {
            k: str(v) if isinstance(v, Decimal) else v
            for (k, v) in dict(agg_bucket).items()
        }

        agg_bucket["source"] = agg_bucket["source"].name
        return agg_bucket

    try:
        db.session.execute("SET LOCAL enable_nestloop TO OFF")
    except OperationalError:
        pass

    for agg_bucket in filter(
        lambda x: product_period_filter(x["ndc11"], x["period"]),
        db.session.execute(query),
    ):
        if period_type == "quarterly" and agg_bucket["period"][4:] not in (
            "03",
            "06",
            "09",
            "12",
        ):
            continue

        elif (
            period_type == "yearly"
            and agg_bucket["period"][4:] != "09"
            and price_type.price_type.name not in ("TEMPNFAMP", "PERMNFAMP")
        ):
            continue

        agg_bucket = _format_bucket(agg_bucket)

        agg_buckets.append(agg_bucket)

    print("agg_buckets_length......", len(agg_buckets))
    print("agg_buckets1000......", agg_buckets[10])

    return agg_buckets

    
def get_formula_report(
    price_type,
    exec_id,
    periods=None,
    period_type=False,
    ignore_method=False,
    method_period=None,
):
    formula_units = aliased(FormulaOutput)

    formula_query = (
        FormulaOutput.query.join(
            Formula,
            and_(
                Formula.id == FormulaOutput.formula_id,
                FormulaOutput.value_type == "DOLLARS",
            ),
        )
        .join(
            formula_units,
            and_(
                Formula.id == formula_units.formula_id,
                formula_units.ndc11 == FormulaOutput.ndc11,
                formula_units.period == FormulaOutput.period,
                formula_units.ratio1 == FormulaOutput.ratio1,
                formula_units.value_type == "UNITS",
            ),
        )
        .filter(FormulaOutput.exec_id == exec_id)
        .filter(formula_units.exec_id == exec_id)
    )

    execution = PriceTypeExecution.query.get(exec_id)
    run_price_type = execution.run_price_type
    product_period_filter = output_util.filter_on_product_period(
        price_type.client_id, run_price_type, price_type
    )

    addl_filters = []
    # max_period = max(periods)
    if periods:
        # rolling_quarters = [periodAdjust(max_period, x) for x in (0, -3, -6, -9)]

        if period_type == "quarterly":
            periods = list(filter(lambda x: x[4:] in ("03", "06", "09", "12"), periods))
        elif period_type == "yearly" and price_type.price_type.name not in (
            "TEMPNFAMP",
            "PERMNFAMP",
        ):
            periods = list(filter(lambda x: x[4:] == "09", periods))

        addl_filters.append(FormulaOutput.period.in_(periods))

    # if ndc_filter and len(ndc_filter) == 9:
    #     addl_filters.append(FormulaOutput.ndc9.contains(ndc_filter))
    # elif ndc_filter and len(ndc_filter) == 11:

    #     # Will filter based on blend for formula_detail requests if possible
    #     using_blend_filter = False
    #     if detail_request_ndc11:
    #         using_blend_filter, formula_query = filter_on_blended_products(
    #             formula_query, price_type, ndc_filter, periods, run_price_type
    #         )

    #     if not using_blend_filter:
    #         addl_filters.append(FormulaOutput.ndc11.contains(ndc_filter))

    # bucket_type_map = {
    #     "summary": Formula.summary.is_(True),
    #     "summary_variance": Formula.summary_variance.is_(True),
    #     "reportable": Formula.reportable.is_(True),
    #     "adhoc": Formula.name.in_(adhoc_buckets),
    # }

    # if bucket_type in bucket_type_map:
    #     addl_filters.append(bucket_type_map[bucket_type])

    addl_filters.append(Formula.derived.is_(False))

    # methods = (False,)
    price_type_enum = price_type.price_type
    def _calc_year(period):
        if price_type_enum in (PriceTypeEnum.TEMPNFAMP, PriceTypeEnum.PERMNFAMP):
            # Only one true period exists for each product, so the value being used
            # as period component in group_by doesn't really matter
            year = max(periods)
        else:
            year = str(int(period[:4]) + 1) if period[4:] > "09" else period[:4]

        return year

    periods = [
        (
            _calc_year(period),
            period[:4] + "Q" + str(ceil(int(period[4:]) / 3)),
            period,
        )
        for period in periods
    ]

    periods_table = union_all(
        *[
            select(
                [
                    literal(period[2]).cast(db.String).label("period"),
                    literal(period[1]).cast(db.String).label("quarter"),
                    literal(period[0]).cast(db.String).label("year"),
                    literal(period[2][:4]+ '-' +period[2][4:]+ '-'+'01').label("date"),
                    literal(period[2][:4]+ '-' +period[2][4:]).label("month"),
                ]
            )
            for period in periods
        ]
    ).cte()

    formula_query = formula_query.join(
        GPPriceType, GPPriceType.id == price_type.id,
    ).join(
        Client, Client.id == GPPriceType.client_id
    )

    formula_query = formula_query.join(
        periods_table, periods_table.c.period == formula_units.period
    )

    formula_query = formula_query.filter(*addl_filters)

    if not ignore_method:
        formula_query = filter_on_ratio(
            formula_query, FormulaOutput, price_type, exec_id, method_period
        )

    # notes = get_notes(exec_id, FormulaOutput, "formula")

    # When running on PostgreSQL force the query planner to ignore nested loop
    # joins in favor of indexes (only for this query).  The planner was mistakenly
    # using nested loops because the table stats are sometimes out of date -- the
    # result is an extremely slow query.

    # The try/except turns this into a noop for SQLite.
    try:
        db.session.execute("SET LOCAL enable_nestloop TO OFF")
    except OperationalError:
        pass

    # print("formula_query......", formula_query)

    def _results_filter(rec):
        # For formula detail requests requiring quarterly formulas,
        # intermediate months should be skipped as they are misleading
        # Detail requests with 12 month formulas should only display the current
        # period for the same reason
        # formula_period = rec[13]
        # formula_level = rec[14]
        dollars = rec[11]
        # if detail_request_ndc11:
        #     if formula_period == 2 and dollars.period not in rolling_quarters:
        #         return False
        #     elif formula_period == 11 and dollars.period != max_period:
        #         return False

        #     # Formulas at the NDC9 are equal across the family.  Only need the version
        #     # corresponding to the requested NDC11
        #     if formula_level.name == "NDC9" and dollars.ndc11 != detail_request_ndc11:
        #         return False

        return product_period_filter(dollars.ndc11, dollars.period)

    price_types_ratio = False
    amp_ratio = False
    
    if price_type.price_type == PriceTypeEnum.AMP:
        amp_ratio = True

    if price_type.price_type in (
        PriceTypeEnum.AMP,
        PriceTypeEnum.NFAMP,
        PriceTypeEnum.ANFAMP,
        PriceTypeEnum.TEMPNFAMP,
        PriceTypeEnum.PERMNFAMP,
    ):
        price_types_ratio = True

    output = []
    for (
        formula_id,
        name,
        reportable,
        ordering,
        threshold_dollar,
        threshold_unit,
        variance,
        derived,
        d_round,
        u_round,
        value_period,
        dollars,
        units_value,
        formula_period,
        formula_level,
        quarter,
        date,
        month,
        year,
        price_type_id,
        price_type_name,
        client,
        method
    ) in filter(
        _results_filter,
        formula_query.with_entities(
            Formula.id,
            Formula.name,
            Formula.reportable_name,
            Formula.ordering,
            Formula.variance_threshold_dollar,
            Formula.variance_threshold_unit,
            Formula.summary_variance,
            Formula.derived,
            Formula.d_round,
            Formula.u_round,
            Formula.value_period,
            FormulaOutput,
            formula_units.value,
            Formula.period,
            Formula.level,
            "quarter",
            "date",
            "month",
            "year",
            GPPriceType.id,
            GPPriceType.name,
            Client.name,
            case(
                    [
                       (literal(price_types_ratio),
                        case(
                            [
                                (literal(amp_ratio),
                                case(
                                    [
                                      (FormulaOutput.ratio1.is_(True), "5i")
                                    ],
                                    else_="RCP",
                                )
                                )
                            ],
                            else_=case(
                                [
                                    (FormulaOutput.ratio1.is_(True), "DS")
                                ],
                                else_="WHL",
                            )
                        )
                       )
                    ],
                    else_="",
                ).label("method")
        ).all(),
    ):

        output.append(
            {
                "id": formula_id,
                "bucket": name,
                "order": ordering,
                "ndc11": dollars.ndc11,
                "ndc9": dollars.ndc9,
                "period": dollars.period,
                "amt": "{:f}".format(dollars.value),
                "units": "{:f}".format(units_value),
                # "count": "N/A",
                "month":month,
                "date":date,
                "quarter":quarter,
                "year":year,
                "calc_id":price_type_id,
                "calc": price_type_name,
                "client": client,
                "source": "",
                "method":method
            }
        )
    
    print("output_length......", len(output))
    print("output......", output[10])
    return output


def get_ratio_out_report(
    price_type,
    exec_id,
    periods
):
    price_type_enum = price_type.price_type
    
    def _calc_year(period):
        if price_type_enum in (PriceTypeEnum.TEMPNFAMP, PriceTypeEnum.PERMNFAMP):
            # Only one true period exists for each product, so the value being used
            # as period component in group_by doesn't really matter
            year = max(periods)
        else:
            year = str(int(period[:4]) + 1) if period[4:] > "09" else period[:4]

        return year

    periods = [
        (
            _calc_year(period),
            period[:4] + "Q" + str(ceil(int(period[4:]) / 3)),
            period,
        )
        for period in periods
    ]

    periods_table = union_all(
        *[
            select(
                [
                    literal(period[2]).cast(db.String).label("period"),
                    literal(period[1]).cast(db.String).label("quarter"),
                    literal(period[0]).cast(db.String).label("year"),
                    literal(period[2][:4]+ '-' +period[2][4:]+ '-'+'01').label("date"),
                    literal(period[2][:4]+ '-' +period[2][4:]).label("month"),
                ]
            )
            for period in periods
        ]
    ).cte()

    price_types_ratio = False
    amp_ratio = False
    
    if price_type.price_type == PriceTypeEnum.AMP:
        amp_ratio = True

    if price_type.price_type in (
        PriceTypeEnum.AMP,
        PriceTypeEnum.NFAMP,
        PriceTypeEnum.ANFAMP,
        PriceTypeEnum.TEMPNFAMP,
        PriceTypeEnum.PERMNFAMP,
    ):
        price_types_ratio = True

    ratio_output = []

    ratio_query = RatioResults.query.filter(
        RatioResults.exec_id == exec_id
    ).join(
        periods_table, periods_table.c.period == RatioResults.period,
    ).with_entities(
        RatioResults.ndc11,
        RatioResults.ndc9,
        RatioResults.period,
        RatioResults.ratio1,
        "quarter",
        "year",
        "date",
        "month",
        case(
                [
                    (literal(price_types_ratio),
                    case(
                        [
                            (literal(amp_ratio),
                            case(
                                [
                                    (RatioResults.ratio1.is_(True), "5i")
                                ],
                                else_="RCP",
                            )
                            )
                        ],
                        else_=case(
                            [
                                (RatioResults.ratio1.is_(True), "DS")
                            ],
                            else_="WHL",
                        )
                    )
                    )
                ],
                else_="",
            ).label("method")
    )
    for out in ratio_query.all():
        ratio_output.append(
             {
                "id": "",
                "bucket": "",
                "order": 0,
                "ndc11": out.ndc11,
                "ndc9": out.ndc9,
                "period": out.period,
                "amt": int(out.ratio1),
                "units": int(out.ratio1),
                "count": "N/A",
                "month":out.month,
                "date":out.date,
                "quarter":out.quarter,
                "year":out.year,
                # "calc_id":price_type_id,
                # "calc": price_type_name,
                # "client": client,
                "source": "",
                "method":out.method
            }
        )

    print("ratio_length.....", len(ratio_output))    
    print("ratio_output...", ratio_output[0])
    return ratio_output
