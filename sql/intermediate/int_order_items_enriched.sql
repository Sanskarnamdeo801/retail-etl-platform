DROP TABLE IF EXISTS int_order_items_enriched;

CREATE TABLE int_order_items_enriched AS
SELECT
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    CAST(oi.quantity AS INTEGER) AS quantity,
    CAST(oi.unit_price AS NUMERIC(18,2)) AS unit_price,
    COALESCE(CAST(oi.discount_amount AS NUMERIC(18,2)), 0) AS discount_amount,
    COALESCE(CAST(oi.tax_amount AS NUMERIC(18,2)), 0) AS tax_amount,
    (CAST(oi.quantity AS NUMERIC(18,2)) * CAST(oi.unit_price AS NUMERIC(18,2))) AS gross_amount,
    (
        (CAST(oi.quantity AS NUMERIC(18,2)) * CAST(oi.unit_price AS NUMERIC(18,2)))
        - COALESCE(CAST(oi.discount_amount AS NUMERIC(18,2)), 0)
        + COALESCE(CAST(oi.tax_amount AS NUMERIC(18,2)), 0)
    ) AS net_amount,
    o.order_date,
    DATE_TRUNC('month', o.order_date)::date AS order_month,
    CURRENT_TIMESTAMP AS etl_loaded_at
FROM stg_order_items oi
JOIN stg_orders o
    ON oi.order_id = o.order_id
LEFT JOIN stg_products p
    ON oi.product_id = p.product_id;