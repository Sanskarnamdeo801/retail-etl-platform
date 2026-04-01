from pathlib import Path
from sqlalchemy import text

def run_sql_file(engine, file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))

def run_sql_group(engine, sql_files):
    for file_path in sql_files:
        path = Path(file_path)
        if path.exists():
            print(f"Running SQL: {file_path}")
            run_sql_file(engine, str(path))
        else:
            print(f"Skipping missing: {file_path}")

# 🔹 staging tables (DDL)
def run_staging_ddl(engine):
    files = [
        "sql/staging/stg_customers.sql",
        "sql/staging/stg_products.sql",
        "sql/staging/stg_orders.sql",
        "sql/staging/stg_order_items.sql",
        "sql/staging/stg_payments.sql",
        "sql/staging/stg_shipments.sql",
    ]
    run_sql_group(engine, files)

# 🔹 transformations (CTAS queries)
def run_transform_sql(engine):
    files = [
        "sql/intermediate/int_orders_enriched.sql",
        "sql/intermediate/int_order_items_enriched.sql",
        "sql/marts/dim_customers.sql",
        "sql/marts/dim_products.sql",
        "sql/marts/fct_orders.sql",
        "sql/marts/fct_order_items.sql",
        "sql/marts/agg_daily_sales.sql",
        "sql/marts/agg_category_sales.sql",
        "sql/marts/agg_customer_metrics.sql",
    ]
    run_sql_group(engine, files)