from pathlib import Path
import os

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "sales"

def main():
    load_dotenv(PROJECT_ROOT / ".env", override=True)

    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
    ]

    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing .env settings: {', '.join(missing)}")

    if not PARQUET_PATH.exists() or not list(PARQUET_PATH.glob("*.parquet")):
        raise FileNotFoundError(
            f"No Parquet files found in {PARQUET_PATH}"
        )

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="RETAIL_DB",
        schema="STAGING",
        role="ACCOUNTADMIN",
    )

    try:
        cursor = connection.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RETAIL_DB.STAGING.STG_RETAIL_SALES (
                TRANSACTION_ID VARCHAR(50),
                TRANSACTION_DATE DATE,
                PRODUCT_ID VARCHAR(50),
                CUSTOMER_ID VARCHAR(50),
                STORE_ID VARCHAR(50),
                QUANTITY NUMBER(10, 0),
                PAYMENT_METHOD VARCHAR(30),
                ORDER_STATUS VARCHAR(30),
                UNIT_PRICE NUMBER(12, 2),
                UNIT_COST NUMBER(12, 2),
                SALES_AMOUNT NUMBER(14, 2),
                COST_AMOUNT NUMBER(14, 2),
                PROFIT_AMOUNT NUMBER(14, 2)
            )
        """)
        print("Staging table created or already exists: STG_RETAIL_SALES")

        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print("Connected to:", cursor.fetchone())


        parquet_files = list(PARQUET_PATH.glob("*.parquet"))
        df = pd.concat(
            [pd.read_parquet(file) for file in parquet_files],
            ignore_index=True,
        )
        df.columns = [column.upper() for column in df.columns]
        print("Records read from Parquet:", len(df))
        print("Columns read:", len(df.columns))

        print("Parquet source:", PARQUET_PATH)
        print(
            "Parquet files found:",
            len(list(PARQUET_PATH.glob("*.parquet"))),
        )

        success, nchunks, nrows, _ = write_pandas(
            connection,
            df,
            table_name="STG_RETAIL_SALES",
            database="RETAIL_DB",
            schema="STAGING",
            auto_create_table=False,
            overwrite=False,
        )

        print("Snowflake load successful:", success)
        print("Chunks loaded:", nchunks)
        print("Rows loaded:", nrows)

        cursor.close()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
