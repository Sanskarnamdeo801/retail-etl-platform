DROP TABLE IF EXISTS int_orders_enriched;

CREATE TABLE int_orders_enriched AS
SELECT
    o.order_id,
    o.customer_id,
    c.first_name,
    c.last_name,
    CONCAT(COALESCE(c.first_name, ''), ' ', COALESCE(c.last_name, '')) AS customer_full_name,
    LOWER(TRIM(c.email)) AS customer_email,
    c.city,
    c.state,
    c.country,
    o.order_date,
    o.order_status,
    o.channel,
    p.payment_id,
    p.payment_method,
    p.payment_status,
    p.amount AS payment_amount,
    s.shipment_id,
    s.carrier,
    s.shipment_status,
    s.shipment_date,
    s.delivery_date,
    CASE
        WHEN s.shipment_date IS NOT NULL AND s.delivery_date IS NOT NULL
        THEN (s.delivery_date - s.shipment_date)
        ELSE NULL
    END AS delivery_days,
    CASE
        WHEN p.payment_status = 'COMPLETED' THEN 1 ELSE 0
    END AS payment_completed_flag,
    CASE
        WHEN s.shipment_status = 'DELIVERED' THEN 1 ELSE 0
    END AS shipment_delivered_flag,
    EXTRACT(YEAR FROM o.order_date) AS order_year,
    EXTRACT(MONTH FROM o.order_date) AS order_month_num,
    CURRENT_TIMESTAMP AS etl_loaded_at
FROM stg_orders o
LEFT JOIN stg_customers c
    ON o.customer_id = c.customer_id
LEFT JOIN stg_payments p
    ON o.order_id = p.order_id
LEFT JOIN stg_shipments s
    ON o.order_id = s.order_id;