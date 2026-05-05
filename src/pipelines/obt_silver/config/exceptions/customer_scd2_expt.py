customer_scd2_expt = {
    # Primary key
    "customer_scd2__valid_customer_id": "customer_id IS NOT NULL",
    # String hygiene — allow word chars, whitespace, hyphen, dot, comma, @
    "customer_scd2__no_special_chars_name":    "customer_name    NOT RLIKE '[^\\w\\s\\-\\.\\,]'",
    "customer_scd2__no_special_chars_email":   "email            NOT RLIKE '[^\\w\\s\\@\\.\\+\\-\\_]'",
    "customer_scd2__no_special_chars_segment": "customer_segment NOT RLIKE '[^\\w\\s\\-\\.]'",
}
