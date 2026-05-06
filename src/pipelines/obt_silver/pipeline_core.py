from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ── Data quality expectations ─────────────────────────────────────────────────
# Inlined here because Databricks workspace notebooks cannot be imported as
# Python modules (NotebookImportException).  Keep these in sync with the
# corresponding files under config/exceptions/ (source of truth for local dev).

_customer_scd2_expt = {
    "customer_scd2__valid_customer_id":        "customer_id      IS NOT NULL",
    # Hyphen placed last in [...] so it is literal, not a range operator
    "customer_scd2__no_special_chars_name":    "customer_name    NOT RLIKE '[^a-zA-Z0-9 _.,-]'",
    "customer_scd2__no_special_chars_email":   "email            NOT RLIKE '[^a-zA-Z0-9@.+_-]'",
    "customer_scd2__no_special_chars_segment": "customer_segment NOT RLIKE '[^a-zA-Z0-9 _.-]'",
}

_product_scd2_expt = {
    "product_scd2__valid_product_id":          "product_id   IS NOT NULL",
    "product_scd2__no_special_chars_name":     "product_name NOT RLIKE '[^a-zA-Z0-9 _.,()/-]'",
    "product_scd2__no_special_chars_category": "category     NOT RLIKE '[^a-zA-Z0-9 _.-]'",
    "product_scd2__non_negative_unit_price":   "unit_price   >= 0",
}

_orders_clean_expt = {
    "orders_clean__valid_order_id":   "order_id   IS NOT NULL",
    "orders_clean__valid_order_date": "order_date IS NOT NULL",
}

_sales_clean_expt = {
    "sales_clean__valid_order_id":               "order_id        IS NOT NULL",
    "sales_clean__valid_product_id":             "product_id      IS NOT NULL",
    "sales_clean__valid_customer_id":            "customer_id     IS NOT NULL",
    "sales_clean__non_negative_quantity":        "quantity        >= 0",
    "sales_clean__non_negative_discount":        "discount        >= 0",
    "sales_clean__non_negative_gross_sales":     "gross_sales     >= 0",
    "sales_clean__non_negative_discount_amount": "discount_amount >= 0",
    "sales_clean__non_negative_net_sales":       "net_sales       >= 0",
}

# ══════════════════════════════════════════════════════════════════════════════
# SOURCE VIEWS
# Bronze → Silver: standardise types, strip special chars, round floats to
# 2 d.p., parse dates.  Bad rows are dropped before the SCD merge.
# ══════════════════════════════════════════════════════════════════════════════

@dp.expect_all_or_drop(_customer_scd2_expt)
@dp.view
def v_bronze_customer():
    return (
        spark.readStream
        .table("claude_aidevkit.bronze.bronze_customer")
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.regexp_replace(F.trim(F.col("customer_name")), r"[^\w\s\-\.\,]", "").alias("customer_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.regexp_replace(F.trim(F.col("customer_segment")), r"[^\w\s\-\.]", "").alias("customer_segment"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(_product_scd2_expt)
@dp.view
def v_bronze_product():
    return (
        spark.readStream
        .table("claude_aidevkit.bronze.bronze_product")
        .select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.regexp_replace(F.trim(F.col("product_name")), r"[^\w\s\-\.\,\(\)\/]", "").alias("product_name"),
            F.regexp_replace(F.trim(F.col("category")), r"[^\w\s\-\.]", "").alias("category"),
            F.round(F.col("unit_price").cast("double"), 2).alias("unit_price"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(_orders_clean_expt)
@dp.view
def v_bronze_order():
    return (
        spark.readStream
        .table("claude_aidevkit.bronze.bronze_order")
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.to_date(F.col("order_date"), "yyyy-MM-dd").alias("order_date"),
            F.col("_ingested_at"),
        )
    )


@dp.expect_all_or_drop(_sales_clean_expt)
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
# • customer / product  → SCD Type 2  (preserve full attribute history)
# • orders / sales      → SCD Type 1  (latest state; dedup via MERGE keys)
# • _ingested_at drives sequence_by; excluded from target (bronze audit col)
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
