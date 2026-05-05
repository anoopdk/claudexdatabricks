dim_product_expt = {
    # Surrogate + business key
    "dim_product__valid_product_sk":    "product_sk     IS NOT NULL",
    "dim_product__valid_product_id":    "product_id     IS NOT NULL",
    # SCD2 effective date must always be set
    "dim_product__valid_effective_date": "effective_date IS NOT NULL",
    # Non-negative price carried from silver core
    "dim_product__non_negative_unit_price": "unit_price >= 0",
}
