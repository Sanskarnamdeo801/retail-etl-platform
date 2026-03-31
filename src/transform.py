import pandas as pd

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["email"] = df["email"].replace("", None)
    df["email"] = df["email"].astype("string").str.strip().str.lower()
    df["first_name"] = df["first_name"].astype("string").str.strip()
    df["last_name"] = df["last_name"].astype("string").str.strip()
    df = df.drop_duplicates(subset=["customer_id"], keep="last")
    return df

def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["product_name"] = df["product_name"].astype("string").str.strip()
    df["category"] = df["category"].astype("string").str.strip()
    df["sub_category"] = df["sub_category"].astype("string").str.strip()
    df["brand"] = df["brand"].astype("string").str.strip()
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
    df = df.drop_duplicates(subset=["product_id"], keep="last")
    return df

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_status"] = df["order_status"].astype("string").str.strip().str.upper()
    df["channel"] = df["channel"].astype("string").str.strip().str.upper()
    return df

def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
    df["discount_amount"] = pd.to_numeric(df["discount_amount"], errors="coerce").fillna(0)
    df["tax_amount"] = pd.to_numeric(df["tax_amount"], errors="coerce").fillna(0)
    return df

def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["payment_method"] = (
        df["payment_method"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace({
            "CREDIT CARD": "CREDIT_CARD",
            "CREDITCARD": "CREDIT_CARD",
            "UPI": "UPI"
        })
    )
    df["payment_status"] = df["payment_status"].astype("string").str.strip().str.upper().fillna("UNKNOWN")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    return df

def clean_shipments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["shipment_status"] = df["shipment_status"].astype("string").str.strip().str.upper().fillna("PENDING")
    return df

def build_int_orders_enriched(orders, customers, payments, shipments):
    df = orders.merge(customers, on="customer_id", how="left", suffixes=("", "_cust"))
    df = df.merge(payments, on="order_id", how="left", suffixes=("", "_pay"))
    df = df.merge(shipments, on="order_id", how="left", suffixes=("", "_ship"))

    df["customer_full_name"] = (
        df["first_name"].fillna("").astype(str).str.strip() + " " +
        df["last_name"].fillna("").astype(str).str.strip()
    ).str.strip()

    df["customer_email"] = df["email"]
    df["payment_completed_flag"] = (df["payment_status"] == "COMPLETED").astype(int)
    df["shipment_delivered_flag"] = (df["shipment_status"] == "DELIVERED").astype(int)

    df["shipment_date"] = pd.to_datetime(df["shipment_date"], errors="coerce")
    df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    df["delivery_days"] = (df["delivery_date"] - df["shipment_date"]).dt.days
    df["order_year"] = df["order_date"].dt.year
    df["order_month_num"] = df["order_date"].dt.month

    return df[[
        "order_id", "customer_id", "customer_full_name", "customer_email",
        "city", "state", "country", "order_date", "order_status", "channel",
        "payment_method", "payment_status", "amount", "carrier",
        "shipment_status", "shipment_date", "delivery_date", "delivery_days",
        "payment_completed_flag", "shipment_delivered_flag", "order_year", "order_month_num"
    ]].rename(columns={"amount": "payment_amount"})

def build_int_order_items_enriched(order_items, orders, products):
    df = order_items.merge(orders[["order_id", "customer_id", "order_date"]], on="order_id", how="left")
    df = df.merge(products[["product_id", "product_name", "category", "sub_category", "brand"]], on="product_id", how="left")

    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] - df["discount_amount"] + df["tax_amount"]
    df["order_month"] = pd.to_datetime(df["order_date"], errors="coerce").dt.to_period("M").dt.to_timestamp()

    return df[[
        "order_item_id", "order_id", "customer_id", "product_id",
        "product_name", "category", "sub_category", "brand",
        "quantity", "unit_price", "discount_amount", "tax_amount",
        "gross_amount", "net_amount", "order_date", "order_month"
    ]]