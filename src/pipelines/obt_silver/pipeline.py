from pyspark import pipelines as dp
from pyspark.sql import functions as F

from config.exceptions.customer_scd2_expt import customer_scd2_expt
from config.exceptions.product_scd2_expt import product_scd2_expt
from config.exceptions.orders_clean_expt import orders_clean_expt
from config.exceptions.sales_clean_expt import sales_clean_expt
from config.exceptions.dim_customer_expt import dim_customer_expt
from config.exceptions.dim_product_expt import dim_product_expt
from config.exceptions.dim_orders_expt import dim_orders_expt
from config.exceptions.fact_sales_expt import fact_sales_expt

# ══════════════════════════════════════════════════════════════════════════════
# SOURCE VIEWS
# Bronze → Silver transformation: standardise types, strip special chars,
# round floats to 2 d.p., parse dates.  Bad rows are quarantined (dropped)
# here so they never reach the SCD merge or the dimensional layer.
# ══════════════════════════════════════════════════════════════════════════════

@dp.expect_all_or_drop(customer_scd2_expt)
@dp.view
def v_bronze_customer():
    return (
        spark.readStream
        .table("claude_aidevkit.bronze.bronze_customer")
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.regexp_replace(
                F.trim(F.col("customer_name")), r"[^\w\s\-\.\,]", ""
            ).alias("customer_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.regexp_replace(
                F.trim(F.col("customer_segment")), r"[^\w\s\-\.]", ""
            ).alias("customer_segment"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(product_scd2_expt)
@dp.view
def v_bronze_product():
    return (
        spark.readStream
        .table("claude_aidevkit.bronze.bronze_product")
        .select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.regexp_replace(
                F.trim(F.col("product_name")), r"[^\w\s\-\.\,\(\)\/]", ""
            ).alias("product_name"),
            F.regexp_replace(
                F.trim(F.col("category")), r"[^\w\s\-\.]", ""
            ).alias("category"),
            F.round(F.col("unit_price").cast("double"), 2).alias("unit_price"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(orders_clean_expt)
@dp.view
def v_bronze_order():
    return (
        spark.readStream
        .table("claude_aidevkit.bronze.bronze_order")
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            # NULL result means unparseable date — caught by orders_clean__valid_order_date
            F.to_date(F.col("order_date"), "yyyy-MM-dd").alias("order_date"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(sales_clean_expt)
@dp.view
def v_bronze_sales():
    return (
        spark.readStream
        .table("claude_aidevkit.bronze.bronze_sales")
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.col("quantity"),
            F.round(F.col("discount").cast("double"),        2).alias("discount"),
            F.round(F.col("gross_sales").cast("double"),     2).alias("gross_sales"),
            F.round(F.col("discount_amount").cast("double"), 2).alias("discount_amount"),
            F.round(F.col("net_sales").cast("double"),       2).alias("net_sales"),
            F.col("_ingested_at"),
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# SILVER CORE — Enriched Layer
#
# Design decisions:
#   • customer / product  → SCD Type 2  (preserve full attribute history)
#   • orders / sales      → SCD Type 1  (latest state; dedup via MERGE keys)
#   • _ingested_at drives sequence_by; excluded from target (bronze audit col)
#   • Late-arriving data is handled automatically: apply_changes re-evaluates
#     the sequence and inserts/closes SCD2 rows as needed.
# ══════════════════════════════════════════════════════════════════════════════

dp.create_streaming_table(
    "claude_aidevkit.silver.customer_scd2",
    comment="SCD Type 2 customer master — full attribute history keyed on customer_id",
    cluster_by=["customer_id"],
)

dp.apply_changes(
    target="claude_aidevkit.silver.customer_scd2",
    source="v_bronze_customer",
    keys=["customer_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=2,
    except_column_list=["_ingested_at"],
)


dp.create_streaming_table(
    "claude_aidevkit.silver.product_scd2",
    comment="SCD Type 2 product master — full attribute history keyed on product_id",
    cluster_by=["product_id"],
)

dp.apply_changes(
    target="claude_aidevkit.silver.product_scd2",
    source="v_bronze_product",
    keys=["product_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=2,
    except_column_list=["_ingested_at"],
)


dp.create_streaming_table(
    "claude_aidevkit.silver.orders_clean",
    comment="Deduplicated, standardised orders — latest state per order_id (SCD Type 1)",
    cluster_by=["order_date"],
)

dp.apply_changes(
    target="claude_aidevkit.silver.orders_clean",
    source="v_bronze_order",
    keys=["order_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
    except_column_list=["_ingested_at"],
)


dp.create_streaming_table(
    "claude_aidevkit.silver.sales_clean",
    comment="Deduplicated, standardised sales line items — latest state per (order_id, product_id)",
    cluster_by=["order_id"],
)

dp.apply_changes(
    target="claude_aidevkit.silver.sales_clean",
    source="v_bronze_sales",
    keys=["order_id", "product_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
    except_column_list=["_ingested_at"],
)


# ══════════════════════════════════════════════════════════════════════════════
# SILVER DIMENSIONAL — Star-Schema Serving Layer
#
# Design decisions:
#   • Surrogate keys: SHA2(business_key || effective_date, 256) — deterministic
#     and reproducible across pipeline runs.
#   • dim_customer / dim_product include ALL SCD2 rows (current + historical)
#     so that fact_sales can resolve the correct version at order_date.
#   • fact_sales joins to SCD2 history on date-range:
#       order_date >= effective_date AND (expiry_date IS NULL OR order_date < expiry_date)
#   • Dimensional tables are materialized views — full refresh on each pipeline run.
# ══════════════════════════════════════════════════════════════════════════════

@dp.expect_all_or_drop(dim_customer_expt)
@dp.create_materialized_view(
    name="claude_aidevkit.silver.dim_customer",
    comment="Customer dimension — all SCD2 versions with surrogate key",
)
def dim_customer():
    return spark.sql("""
        SELECT
            SHA2(CONCAT(customer_id, CAST(__start_at AS STRING)), 256) AS customer_sk,
            customer_id,
            customer_name,
            email,
            customer_segment,
            CAST(__start_at AS DATE) AS effective_date,
            CAST(__end_at   AS DATE) AS expiry_date,
            __is_current             AS is_current
        FROM claude_aidevkit.silver.customer_scd2
    """)


@dp.expect_all_or_drop(dim_product_expt)
@dp.create_materialized_view(
    name="claude_aidevkit.silver.dim_product",
    comment="Product dimension — all SCD2 versions with surrogate key",
)
def dim_product():
    return spark.sql("""
        SELECT
            SHA2(CONCAT(product_id, CAST(__start_at AS STRING)), 256) AS product_sk,
            product_id,
            product_name,
            category,
            unit_price,
            CAST(__start_at AS DATE) AS effective_date,
            CAST(__end_at   AS DATE) AS expiry_date,
            __is_current             AS is_current
        FROM claude_aidevkit.silver.product_scd2
    """)


@dp.expect_all_or_drop(dim_orders_expt)
@dp.create_materialized_view(
    name="claude_aidevkit.silver.dim_orders",
    comment="Order dimension — one row per order with calendar attributes",
)
def dim_orders():
    return spark.sql("""
        SELECT
            SHA2(order_id, 256) AS order_sk,
            order_id,
            order_date,
            YEAR(order_date)    AS order_year,
            QUARTER(order_date) AS order_quarter,
            MONTH(order_date)   AS order_month
        FROM claude_aidevkit.silver.orders_clean
    """)


@dp.expect_all_or_drop(fact_sales_expt)
@dp.create_materialized_view(
    name="claude_aidevkit.silver.fact_sales",
    comment="Sales fact table — measures at order-product grain, foreign keys to all dimensions",
)
def fact_sales():
    return spark.sql("""
        SELECT
            sc.order_id,
            o.order_date,
            SHA2(sc.order_id, 256)                                                AS order_sk,
            -- Resolve the SCD2 customer version active at order_date
            SHA2(CONCAT(sc.customer_id, CAST(dc.__start_at AS STRING)), 256)      AS customer_sk,
            -- Resolve the SCD2 product version active at order_date
            SHA2(CONCAT(sc.product_id,  CAST(dp.__start_at AS STRING)), 256)      AS product_sk,
            sc.quantity,
            sc.discount,
            sc.gross_sales,
            sc.discount_amount,
            sc.net_sales
        FROM claude_aidevkit.silver.sales_clean sc
        INNER JOIN claude_aidevkit.silver.orders_clean o
            ON sc.order_id = o.order_id
        -- Date-range join: pick the customer SCD2 row that was active when the order was placed
        LEFT JOIN claude_aidevkit.silver.customer_scd2 dc
            ON  sc.customer_id = dc.customer_id
            AND o.order_date >= CAST(dc.__start_at AS DATE)
            AND (dc.__end_at IS NULL OR o.order_date < CAST(dc.__end_at AS DATE))
        -- Date-range join: pick the product SCD2 row that was active when the order was placed
        LEFT JOIN claude_aidevkit.silver.product_scd2 dp
            ON  sc.product_id = dp.product_id
            AND o.order_date >= CAST(dp.__start_at AS DATE)
            AND (dp.__end_at IS NULL OR o.order_date < CAST(dp.__end_at AS DATE))
    """)
