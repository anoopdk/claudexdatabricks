from pyspark import pipelines as dp
from pyspark.sql import functions as F

from config.exceptions.customer_scd2_expt import customer_scd2_expt
from config.exceptions.product_scd2_expt import product_scd2_expt
from config.exceptions.orders_clean_expt import orders_clean_expt
from config.exceptions.sales_clean_expt import sales_clean_expt

# ══════════════════════════════════════════════════════════════════════════════
# SOURCE VIEWS
# Bronze → Silver: standardise types, strip special chars, round floats to
# 2 d.p., parse dates.  Bad rows are dropped before the SCD merge.
# ══════════════════════════════════════════════════════════════════════════════

@dp.expect_all_or_drop(customer_scd2_expt)
@dp.view
def v_bronze_customer():
    return (
        dp.read("bronze_customer")
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.regexp_replace(F.trim(F.col("customer_name")), r"[^\w\s\-\.\,]", "").alias("customer_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.regexp_replace(F.trim(F.col("customer_segment")), r"[^\w\s\-\.]", "").alias("customer_segment"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(product_scd2_expt)
@dp.view
def v_bronze_product():
    return (
        dp.read("bronze_product")
        .select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.regexp_replace(F.trim(F.col("product_name")), r"[^\w\s\-\.\,\(\)\/]", "").alias("product_name"),
            F.regexp_replace(F.trim(F.col("category")), r"[^\w\s\-\.]", "").alias("category"),
            F.round(F.col("unit_price").cast("double"), 2).alias("unit_price"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(orders_clean_expt)
@dp.view
def v_bronze_order():
    return (
        dp.read("bronze_order")
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.to_date(F.col("order_date"), "yyyy-MM-dd").alias("order_date"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(sales_clean_expt)
@dp.view
def v_bronze_sales():
    return (
        dp.read("bronze_sales")
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
# • customer / product  → SCD Type 2  (preserve full attribute history)
# • orders / sales      → SCD Type 1  (latest state; dedup via MERGE keys)
# • _ingested_at drives sequence_by; excluded from target (bronze audit col)
# ══════════════════════════════════════════════════════════════════════════════

dp.create_streaming_table(
    "claude_aidevkit.silver.customer_scd2",
    comment="SCD Type 2 customer master — full attribute history keyed on customer_id",
    cluster_by=["customer_id"],
)
dp.create_auto_cdc_flow(
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
dp.create_auto_cdc_flow(
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
dp.create_auto_cdc_flow(
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
dp.create_auto_cdc_flow(
    target="claude_aidevkit.silver.sales_clean",
    source="v_bronze_sales",
    keys=["order_id", "product_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
    except_column_list=["_ingested_at"],
)
