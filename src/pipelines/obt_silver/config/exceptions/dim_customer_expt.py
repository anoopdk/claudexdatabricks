dim_customer_expt = {
    # Surrogate + business key
    "dim_customer__valid_customer_sk":    "customer_sk    IS NOT NULL",
    "dim_customer__valid_customer_id":    "customer_id    IS NOT NULL",
    # SCD2 effective date must always be set
    "dim_customer__valid_effective_date": "effective_date IS NOT NULL",
}
