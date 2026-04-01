DROP TABLE IF EXISTS agg_customer_metrics;

CREATE TABLE agg_customer_metrics AS
SELECT
    customer_id,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(net_amount) AS lifetime_value,
    AVG(net_amount) AS avg_order_line_value,
    CASE
        WHEN COUNT(DISTINCT order_id) > 1 THEN 1 ELSE 0
    END AS repeat_customer_flag,
    CURRENT_TIMESTAMP AS etl_loaded_at
FROM int_order_items_enriched
GROUP BY customer_id;