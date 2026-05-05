orders_clean_expt = {
    # Primary key
    "orders_clean__valid_order_id": "order_id IS NOT NULL",
    # NULL after to_date() means source value could not be parsed as yyyy-MM-dd
    "orders_clean__valid_order_date": "order_date IS NOT NULL",
}
