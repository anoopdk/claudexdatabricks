fact_sales_expt = {
    # Grain key + foreign keys
    "fact_sales__valid_order_id":    "order_id    IS NOT NULL",
    "fact_sales__valid_order_sk":    "order_sk    IS NOT NULL",
    "fact_sales__valid_customer_sk": "customer_sk IS NOT NULL",
    "fact_sales__valid_product_sk":  "product_sk  IS NOT NULL",
    # Non-negative measures
    "fact_sales__non_negative_quantity":        "quantity        >= 0",
    "fact_sales__non_negative_discount":        "discount        >= 0",
    "fact_sales__non_negative_gross_sales":     "gross_sales     >= 0",
    "fact_sales__non_negative_discount_amount": "discount_amount >= 0",
    "fact_sales__non_negative_net_sales":       "net_sales       >= 0",
}
