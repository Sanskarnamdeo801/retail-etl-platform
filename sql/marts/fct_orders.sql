DROP TABLE IF EXISTS fct_orders;

CREATE TABLE fct_orders AS
SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    channel,
    payment_method,
    payment_status,
    payment_amount,
    shipment_status,
    delivery_days,
    payment_completed_flag,
    shipment_delivered_flag,
    etl_loaded_at
FROM int_orders_enriched;