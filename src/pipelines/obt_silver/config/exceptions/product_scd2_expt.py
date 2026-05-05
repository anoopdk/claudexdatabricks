product_scd2_expt = {
    # Primary key
    "product_scd2__valid_product_id": "product_id IS NOT NULL",
    # String hygiene
    "product_scd2__no_special_chars_name":     "product_name NOT RLIKE '[^\\w\\s\\-\\.\\,\\(\\)\\/]'",
    "product_scd2__no_special_chars_category": "category     NOT RLIKE '[^\\w\\s\\-\\.]'",
    # Non-negative numeric
    "product_scd2__non_negative_unit_price": "unit_price >= 0",
}
