from dotenv import load_dotenv
from src.db import get_source_engine, get_target_engine
from src.extract import extract_table
from src.transform import (
    clean_customers, clean_products, clean_orders,
    clean_order_items, clean_payments, clean_shipments
)
from src.load import load_dataframe
from src.sql_runner import run_source_ddl, run_staging_ddl, run_transform_sql

load_dotenv()

def run_pipeline():
    src_engine = get_source_engine()
    tgt_engine = get_target_engine()

    
    run_source_ddl(src_engine)

   
    run_staging_ddl(tgt_engine)

   
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

    
    run_transform_sql(tgt_engine)


if __name__ == "__main__":
    run_pipeline()