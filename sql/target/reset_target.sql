DROP TABLE IF EXISTS
    agg_customer_metrics,
    agg_category_sales,
    agg_daily_sales,
    fct_order_items,
    fct_orders,
    dim_products,
    dim_customers,
    int_order_items_enriched,
    int_orders_enriched,
    stg_shipments,
    stg_payments,
    stg_order_items,
    stg_orders,
    stg_products,
    stg_customers
CASCADE;