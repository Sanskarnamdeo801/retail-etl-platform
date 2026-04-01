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