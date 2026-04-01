from dotenv import load_dotenv
from src.db import get_source_engine, get_target_engine
from src.extract import extract_table
from src.transform import (
    clean_customers,
    clean_products,
    clean_orders,
    clean_order_items,
    clean_payments,
    clean_shipments,
)
from src.load import load_dataframe
from src.sql_runner import (
    reset_source_tables,
    reset_target_tables,
    run_source_ddl,
    run_staging_ddl,
    run_transform_sql,
)

from src.generate_data import generate_all_data

load_dotenv()


def run_pipeline():
    src_engine = get_source_engine()
    tgt_engine = get_target_engine()

    # 🔥 STEP 0: RESET SOURCE
    print("Step 0: Resetting source tables...")
    reset_source_tables(src_engine)

    # 🔥 STEP 1: CREATE SOURCE TABLES
    print("Step 1: Creating source tables...")
    run_source_ddl(src_engine)

    # 🔥 IMPORTANT: Ensure tables are committed before insert
    print("Source tables created successfully")

    # 🔥 STEP 2: GENERATE DATA (NOW SAFE)
    print("Step 2: Generating synthetic source data...")
    generate_all_data()

    # 🔥 STEP 3: RESET TARGET
    print("Step 3: Resetting target tables...")
    reset_target_tables(tgt_engine)

    # 🔥 STEP 4: CREATE STAGING
    print("Step 4: Creating target staging tables...")
    run_staging_ddl(tgt_engine)

    # 🔥 STEP 5: EXTRACT + CLEAN
    print("Step 5: Extracting and cleaning source data...")
    customers = clean_customers(extract_table(src_engine, "customers"))
    products = clean_products(extract_table(src_engine, "products"))
    orders = clean_orders(extract_table(src_engine, "orders"))
    order_items = clean_order_items(extract_table(src_engine, "order_items"))
    payments = clean_payments(extract_table(src_engine, "payments"))
    shipments = clean_shipments(extract_table(src_engine, "shipments"))

    # 🔥 STEP 6: LOAD STAGING
    print("Step 6: Loading staging tables...")
    load_dataframe(customers, tgt_engine, "stg_customers", if_exists="replace")
    load_dataframe(products, tgt_engine, "stg_products", if_exists="replace")
    load_dataframe(orders, tgt_engine, "stg_orders", if_exists="replace")
    load_dataframe(order_items, tgt_engine, "stg_order_items", if_exists="replace")
    load_dataframe(payments, tgt_engine, "stg_payments", if_exists="replace")
    load_dataframe(shipments, tgt_engine, "stg_shipments", if_exists="replace")

    # 🔥 STEP 7: TRANSFORM
    print("Step 7: Running SQL transformations...")
    run_transform_sql(tgt_engine)

    print("Pipeline completed successfully 🚀")


if __name__ == "__main__":
    run_pipeline()