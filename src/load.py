def load_dataframe(df, engine, table_name: str, if_exists: str = "append"):
    df.to_sql(table_name, engine, if_exists=if_exists, index=False, method="multi", chunksize=1000)