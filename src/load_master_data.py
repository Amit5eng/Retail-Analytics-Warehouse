from pathlib import Path
import os

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = PROJECT_ROOT / "data" / "raw"

TABLES = {
    "products.csv": {
        "table": "STG_PRODUCTS",
        "ddl": """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.STAGING.STG_PRODUCTS (
                PRODUCT_ID VARCHAR(50),
                PRODUCT_NAME VARCHAR(200),
                CATEGORY VARCHAR(100),
                UNIT_COST NUMBER(12, 2),
                UNIT_PRICE NUMBER(12, 2)
            )
        """,
    },
    "customers.csv": {
        "table": "STG_CUSTOMERS",
        "ddl": """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.STAGING.STG_CUSTOMERS (
                CUSTOMER_ID VARCHAR(50),
                CUSTOMER_NAME VARCHAR(200),
                CITY VARCHAR(100),
                SIGNUP_DATE DATE
            )
        """,
    },
    "stores.csv": {
        "table": "STG_STORES",
        "ddl": """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.STAGING.STG_STORES (
                STORE_ID VARCHAR(50),
                STORE_NAME VARCHAR(200),
                CITY VARCHAR(100),
                REGION VARCHAR(100)
            )
        """,
    },
}


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
        with connection.cursor() as cursor:
            for filename, config in TABLES.items():
                csv_path = RAW_PATH / filename
                if not csv_path.exists():
                    raise FileNotFoundError(f"Missing dataset: {csv_path}")

                df = pd.read_csv(csv_path)
                df.columns = [column.upper() for column in df.columns]

                if filename == "customers.csv":
                    df["SIGNUP_DATE"] = pd.to_datetime(
                        df["SIGNUP_DATE"], errors="raise"
                    ).dt.date

                cursor.execute(config["ddl"])
                cursor.execute(
                    f"TRUNCATE TABLE RETAIL_DB.STAGING.{config['table']}"
                )

                success, chunks, rows, _ = write_pandas(
                    connection,
                    df,
                    table_name=config["table"],
                    database="RETAIL_DB",
                    schema="STAGING",
                    auto_create_table=False,
                    overwrite=False,
                )

                if not success or rows != len(df):
                    raise RuntimeError(
                        f"Load failed for {config['table']}: "
                        f"success={success}, loaded={rows}, expected={len(df)}"
                    )

                print(
                    f"{config['table']}: loaded {rows} rows "
                    f"in {chunks} chunk(s)"
                )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
