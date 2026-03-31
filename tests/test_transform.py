import pandas as pd
from src.transform import clean_customers, clean_order_items

def test_clean_customers():
    df = pd.DataFrame([
        {"customer_id": 1, "first_name": " Amit ", "last_name": " Sharma ", "email": " TEST@MAIL.COM "},
        {"customer_id": 1, "first_name": "Amit", "last_name": "Sharma", "email": "test@mail.com"}
    ])

    result = clean_customers(df)
    assert len(result) == 1
    assert result.iloc[0]["email"] == "test@mail.com"

def test_clean_order_items_nulls():
    df = pd.DataFrame([
        {"order_item_id": 1, "order_id": 101, "product_id": 10, "quantity": 2, "unit_price": 100, "discount_amount": None, "tax_amount": None}
    ])

    result = clean_order_items(df)
    assert result.iloc[0]["discount_amount"] == 0
    assert result.iloc[0]["tax_amount"] == 0