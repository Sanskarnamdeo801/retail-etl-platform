DROP TABLE IF EXISTS fct_order_items;

CREATE TABLE fct_order_items AS
SELECT
    order_item_id,
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    tax_amount,
    gross_amount,
    net_amount,
    order_date,
    order_month,
    etl_loaded_at
FROM int_order_items_enriched;