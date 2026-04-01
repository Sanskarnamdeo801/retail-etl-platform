DROP TABLE IF EXISTS agg_daily_sales;

CREATE TABLE agg_daily_sales AS
SELECT
    order_date::date AS sales_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(net_amount) AS total_sales,
    SUM(quantity) AS total_units_sold,
    AVG(net_amount) AS avg_line_sales,
    CURRENT_TIMESTAMP AS etl_loaded_at
FROM int_order_items_enriched
GROUP BY order_date::date;