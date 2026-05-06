from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ── Data quality expectations (inlined — workspace notebooks cannot be imported) ──

_dim_customer_expt = {
    "dim_customer__valid_customer_sk":    "customer_sk    IS NOT NULL",
    "dim_customer__valid_customer_id":    "customer_id    IS NOT NULL",
    "dim_customer__valid_effective_date": "effective_date IS NOT NULL",
}

_dim_product_expt = {
    "dim_product__valid_product_sk":        "product_sk     IS NOT NULL",
    "dim_product__valid_product_id":        "product_id     IS NOT NULL",
    "dim_product__valid_effective_date":    "effective_date IS NOT NULL",
    "dim_product__non_negative_unit_price": "unit_price     >= 0",
}

_dim_orders_expt = {
    "dim_orders__valid_order_sk":   "order_sk   IS NOT NULL",
    "dim_orders__valid_order_id":   "order_id   IS NOT NULL",
    "dim_orders__valid_order_date": "order_date IS NOT NULL",
}

_fact_sales_expt = {
    "fact_sales__valid_order_id":               "order_id        IS NOT NULL",
    "fact_sales__valid_order_sk":               "order_sk        IS NOT NULL",
    "fact_sales__valid_customer_sk":            "customer_sk     IS NOT NULL",
    "fact_sales__valid_product_sk":             "product_sk      IS NOT NULL",
    "fact_sales__non_negative_quantity":        "quantity        >= 0",
    "fact_sales__non_negative_discount":        "discount        >= 0",
    "fact_sales__non_negative_gross_sales":     "gross_sales     >= 0",
    "fact_sales__non_negative_discount_amount": "discount_amount >= 0",
    "fact_sales__non_negative_net_sales":       "net_sales       >= 0",
}

# ══════════════════════════════════════════════════════════════════════════════
# SILVER DIMENSIONAL — Materialized Views (batch, auto-incremental refresh)
#
# Reads silver core tables via batch spark.read — correct API for MVs.
# Because MVs run batch SQL, fact_sales can use the proper SCD2 date-range
# join to link each sale to the customer/product version active at order_date.
#
# Surrogate key formula: SHA2(CONCAT(biz_key, CAST(__start_at AS STRING)), 256)
# — consistent across dim and fact tables.
# ══════════════════════════════════════════════════════════════════════════════

@dp.materialized_view(name="claude_aidevkit.silver.dim_customer")
@dp.expect_all_or_drop(_dim_customer_expt)
def dim_customer():
    return spark.sql("""
        SELECT
            SHA2(CONCAT(customer_id, CAST(__start_at AS STRING)), 256) AS customer_sk,
            customer_id,
            customer_name,
            email,
            customer_segment,
            CAST(__start_at AS DATE)       AS effective_date,
            CAST(__end_at   AS DATE)       AS expiry_date,
            (__end_at IS NULL)             AS is_current
        FROM claude_aidevkit.silver.customer_scd2
    """)


@dp.materialized_view(name="claude_aidevkit.silver.dim_product")
@dp.expect_all_or_drop(_dim_product_expt)
def dim_product():
    return spark.sql("""
        SELECT
            SHA2(CONCAT(product_id, CAST(__start_at AS STRING)), 256) AS product_sk,
            product_id,
            product_name,
            category,
            unit_price,
            CAST(__start_at AS DATE)       AS effective_date,
            CAST(__end_at   AS DATE)       AS expiry_date,
            (__end_at IS NULL)             AS is_current
        FROM claude_aidevkit.silver.product_scd2
    """)


@dp.materialized_view(name="claude_aidevkit.silver.dim_orders")
@dp.expect_all_or_drop(_dim_orders_expt)
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


@dp.materialized_view(name="claude_aidevkit.silver.fact_sales")
@dp.expect_all_or_drop(_fact_sales_expt)
def fact_sales():
    return spark.sql("""
        SELECT
            sc.order_id,
            o.order_date,
            SHA2(sc.order_id, 256)                                               AS order_sk,
            -- Resolve SCD2 customer version active at order_date (date-range join)
            SHA2(CONCAT(sc.customer_id, CAST(dc.__start_at AS STRING)), 256)     AS customer_sk,
            -- Resolve SCD2 product version active at order_date (date-range join)
            SHA2(CONCAT(sc.product_id,  CAST(dp.__start_at AS STRING)), 256)     AS product_sk,
            sc.quantity,
            sc.discount,
            sc.gross_sales,
            sc.discount_amount,
            sc.net_sales
        FROM claude_aidevkit.silver.sales_clean sc
        INNER JOIN claude_aidevkit.silver.orders_clean o
            ON sc.order_id = o.order_id
        LEFT JOIN claude_aidevkit.silver.customer_scd2 dc
            ON  sc.customer_id = dc.customer_id
            AND o.order_date >= CAST(dc.__start_at AS DATE)
            AND (dc.__end_at IS NULL OR o.order_date < CAST(dc.__end_at AS DATE))
        LEFT JOIN claude_aidevkit.silver.product_scd2 dp
            ON  sc.product_id = dp.product_id
            AND o.order_date >= CAST(dp.__start_at AS DATE)
            AND (dp.__end_at IS NULL OR o.order_date < CAST(dp.__end_at AS DATE))
    """)
