from dotenv import load_dotenv
from src.db import get_source_engine, get_target_engine
from src.extract import extract_table
from src.transform import (
    clean_customers, clean_products, clean_orders,
    clean_order_items, clean_payments, clean_shipments,
    build_int_orders_enriched, build_int_order_items_enriched
)
from src.load import load_dataframe

load_dotenv()

def run_pipeline():
    src_engine = get_source_engine()
    tgt_engine = get_target_engine()

    customers = clean_customers(extract_table(src_engine, "customers"))
    products = clean_products(extract_table(src_engine, "products"))
    orders = clean_orders(extract_table(src_engine, "orders"))
    order_items = clean_order_items(extract_table(src_engine, "order_items"))
    payments = clean_payments(extract_table(src_engine, "payments"))
    shipments = clean_shipments(extract_table(src_engine, "shipments"))

    load_dataframe(customers, tgt_engine, "stg_customers", if_exists="replace")
    load_dataframe(products, tgt_engine, "stg_products", if_exists="replace")
    load_dataframe(orders, tgt_engine, "stg_orders", if_exists="replace")
    load_dataframe(order_items, tgt_engine, "stg_order_items", if_exists="replace")
    load_dataframe(payments, tgt_engine, "stg_payments", if_exists="replace")
    load_dataframe(shipments, tgt_engine, "stg_shipments", if_exists="replace")

    int_orders = build_int_orders_enriched(orders, customers, payments, shipments)
    int_order_items = build_int_order_items_enriched(order_items, orders, products)

    load_dataframe(int_orders, tgt_engine, "int_orders_enriched", if_exists="replace")
    load_dataframe(int_order_items, tgt_engine, "int_order_items_enriched", if_exists="replace")

    dim_customers = customers.copy()
    dim_customers["full_name"] = (dim_customers["first_name"].fillna("") + " " + dim_customers["last_name"].fillna("")).str.strip()
    dim_customers = dim_customers[["customer_id", "full_name", "email", "city", "state", "country", "created_at"]]
    load_dataframe(dim_customers, tgt_engine, "dim_customers", if_exists="replace")

    dim_products = products[["product_id", "product_name", "category", "sub_category", "brand", "unit_price", "is_active"]]
    load_dataframe(dim_products, tgt_engine, "dim_products", if_exists="replace")

    fct_orders = int_orders[[
        "order_id", "customer_id", "order_date", "order_status", "channel",
        "payment_method", "payment_status", "payment_amount", "shipment_status",
        "delivery_days", "payment_completed_flag", "shipment_delivered_flag"
    ]]
    load_dataframe(fct_orders, tgt_engine, "fct_orders", if_exists="replace")

    fct_order_items = int_order_items[[
        "order_item_id", "order_id", "customer_id", "product_id", "quantity",
        "unit_price", "discount_amount", "tax_amount", "gross_amount", "net_amount",
        "order_date", "order_month"
    ]]
    load_dataframe(fct_order_items, tgt_engine, "fct_order_items", if_exists="replace")

    agg_daily_sales = (
        int_order_items.groupby(int_order_items["order_date"].dt.date, dropna=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_customers=("customer_id", "nunique"),
            total_sales=("net_amount", "sum"),
            total_units_sold=("quantity", "sum"),
            avg_line_sales=("net_amount", "mean")
        )
        .reset_index()
        .rename(columns={"order_date": "sales_date"})
    )
    load_dataframe(agg_daily_sales, tgt_engine, "agg_daily_sales", if_exists="replace")

    agg_category_sales = (
        int_order_items.groupby(["order_month", "category"], dropna=False)
        .agg(
            total_units=("quantity", "sum"),
            gross_sales=("gross_amount", "sum"),
            total_discount=("discount_amount", "sum"),
            total_tax=("tax_amount", "sum"),
            net_sales=("net_amount", "sum")
        )
        .reset_index()
    )
    load_dataframe(agg_category_sales, tgt_engine, "agg_category_sales", if_exists="replace")

    agg_customer_metrics = (
        int_order_items.groupby("customer_id", dropna=False)
        .agg(
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
            total_orders=("order_id", "nunique"),
            lifetime_value=("net_amount", "sum"),
            avg_order_line_value=("net_amount", "mean")
        )
        .reset_index()
    )
    agg_customer_metrics["repeat_customer_flag"] = (agg_customer_metrics["total_orders"] > 1).astype(int)
    load_dataframe(agg_customer_metrics, tgt_engine, "agg_customer_metrics", if_exists="replace")

if __name__ == "__main__":
    run_pipeline()