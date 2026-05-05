dim_orders_expt = {
    # Surrogate + business key
    "dim_orders__valid_order_sk": "order_sk   IS NOT NULL",
    "dim_orders__valid_order_id": "order_id   IS NOT NULL",
    # Date
    "dim_orders__valid_order_date": "order_date IS NOT NULL",
}
