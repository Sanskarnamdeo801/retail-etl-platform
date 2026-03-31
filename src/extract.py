import pandas as pd

def extract_table(engine, table_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)