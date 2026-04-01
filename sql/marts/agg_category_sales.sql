DROP TABLE IF EXISTS agg_category_sales;

CREATE TABLE agg_category_sales AS
SELECT
    order_month,
    category,
    SUM(quantity) AS total_units,
    SUM(gross_amount) AS gross_sales,
    SUM(discount_amount) AS total_discount,
    SUM(tax_amount) AS total_tax,
    SUM(net_amount) AS net_sales,
    CURRENT_TIMESTAMP AS etl_loaded_at
FROM int_order_items_enriched
GROUP BY order_month, category;