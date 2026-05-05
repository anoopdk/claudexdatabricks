sales_clean_expt = {
    # Composite primary key
    "sales_clean__valid_order_id":    "order_id    IS NOT NULL",
    "sales_clean__valid_product_id":  "product_id  IS NOT NULL",
    "sales_clean__valid_customer_id": "customer_id IS NOT NULL",
    # Non-negative measures
    "sales_clean__non_negative_quantity":        "quantity        >= 0",
    "sales_clean__non_negative_discount":        "discount        >= 0",
    "sales_clean__non_negative_gross_sales":     "gross_sales     >= 0",
    "sales_clean__non_negative_discount_amount": "discount_amount >= 0",
    "sales_clean__non_negative_net_sales":       "net_sales       >= 0",
}
