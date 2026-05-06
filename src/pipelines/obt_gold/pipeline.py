from pyspark import pipelines as dp

# ══════════════════════════════════════════════════════════════════════════════
# GOLD LAYER — Business Aggregations
#
# Reads exclusively from silver dimensional tables (fact_sales, dim_customer,
# dim_product, dim_orders).  Uses @dp.materialized_view for all tables:
#   • batch spark.read semantics — correct for aggregations
#   • auto-incremental refresh by Databricks
#   • no streaming or SCD logic here; that is handled in silver
#
# Joining fact_sales.customer_sk → dim_customer.customer_sk gives the correct
# historical customer version (SCD2-aware) without any extra logic.
# ══════════════════════════════════════════════════════════════════════════════


@dp.materialized_view(name="claude_aidevkit.gold.monthly_revenue_by_segment")
def monthly_revenue_by_segment():
    """Monthly net revenue and order volume broken down by customer segment.

    Demonstrates using the SCD2-aware customer_sk join so the segment label
    reflects what the customer's segment was at the time of purchase.
    """
    return spark.sql("""
        SELECT
            d.order_year,
            d.order_month,
            c.customer_segment,
            COUNT(DISTINCT f.order_id)           AS total_orders,
            COUNT(DISTINCT f.customer_sk)        AS unique_customers,
            SUM(f.quantity)                      AS total_units_sold,
            ROUND(SUM(f.gross_sales),     2)     AS gross_revenue,
            ROUND(SUM(f.discount_amount), 2)     AS total_discounts,
            ROUND(SUM(f.net_sales),       2)     AS net_revenue,
            ROUND(
                SUM(f.discount_amount) / NULLIF(SUM(f.gross_sales), 0) * 100,
            2)                                   AS discount_rate_pct
        FROM claude_aidevkit.silver.fact_sales f
        -- dim_orders provides pre-computed calendar attributes
        JOIN claude_aidevkit.silver.dim_orders d
            ON f.order_sk = d.order_sk
        -- SCD2-aware join: customer_sk matches the version active at order date
        JOIN claude_aidevkit.silver.dim_customer c
            ON f.customer_sk = c.customer_sk
        GROUP BY
            d.order_year,
            d.order_month,
            c.customer_segment
        ORDER BY
            d.order_year,
            d.order_month,
            c.customer_segment
    """)


@dp.materialized_view(name="claude_aidevkit.gold.product_performance")
def product_performance():
    """Product-level sales performance metrics grouped by category.

    Demonstrates using the SCD2-aware product_sk join so price and category
    reflect the product's attributes at the time of sale.
    """
    return spark.sql("""
        SELECT
            p.category,
            p.product_id,
            p.product_name,
            ROUND(p.unit_price, 2)               AS listed_unit_price,
            COUNT(DISTINCT f.order_id)           AS total_orders,
            SUM(f.quantity)                      AS units_sold,
            ROUND(SUM(f.gross_sales),     2)     AS gross_revenue,
            ROUND(SUM(f.discount_amount), 2)     AS total_discounts,
            ROUND(SUM(f.net_sales),       2)     AS net_revenue,
            ROUND(AVG(f.discount),        2)     AS avg_discount_rate,
            ROUND(
                SUM(f.net_sales) / NULLIF(SUM(f.quantity), 0),
            2)                                   AS avg_net_revenue_per_unit
        FROM claude_aidevkit.silver.fact_sales f
        -- SCD2-aware join: product_sk matches the version active at order date
        JOIN claude_aidevkit.silver.dim_product p
            ON f.product_sk = p.product_sk
        GROUP BY
            p.category,
            p.product_id,
            p.product_name,
            p.unit_price
        ORDER BY
            p.category,
            net_revenue DESC
    """)
