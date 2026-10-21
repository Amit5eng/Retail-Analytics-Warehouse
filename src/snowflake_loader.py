
from pathlib import Path
import os

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "sales"

DATABASE = "RETAIL_DB"
SCHEMA = "STAGING"
TARGET_TABLE = "STG_RETAIL_SALES"
TEMP_TABLE = "TEMP_STG_RETAIL_SALES"

COLUMNS = [
    "TRANSACTION_ID",
    "TRANSACTION_DATE",
    "PRODUCT_ID",
    "CUSTOMER_ID",
    "STORE_ID",
    "QUANTITY",
    "PAYMENT_METHOD",
    "ORDER_STATUS",
    "UNIT_PRICE",
    "UNIT_COST",
    "SALES_AMOUNT",
    "COST_AMOUNT",
    "PROFIT_AMOUNT",
]


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
        raise ValueError(
            f"Missing .env settings: {', '.join(missing)}"
        )

    parquet_files = sorted(PARQUET_PATH.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in {PARQUET_PATH}"
        )

    df = pd.concat(
        [pd.read_parquet(file) for file in parquet_files],
        ignore_index=True,
    )

    df.columns = [column.upper() for column in df.columns]

    missing_columns = sorted(set(COLUMNS) - set(df.columns))
    if missing_columns:
        raise ValueError(
            f"Missing required source columns: {missing_columns}"
        )

    df = df[COLUMNS]

    if df.empty:
        raise ValueError("No sales records found in the Parquet data.")

    if df["TRANSACTION_ID"].isna().any():
        raise ValueError("Source contains null transaction IDs.")

    if df["TRANSACTION_ID"].duplicated().any():
        raise ValueError("Source contains duplicate transaction IDs.")

    print("Parquet files:", len(parquet_files))
    print("Source records:", len(df))
    print("Unique transaction IDs:", df["TRANSACTION_ID"].nunique())

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=DATABASE,
        schema=SCHEMA,
        role=os.getenv("SNOWFLAKE_ROLE") or "ACCOUNTADMIN",
    )

    cursor = None

    try:
        cursor = connection.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS
                {DATABASE}.{SCHEMA}.{TARGET_TABLE} (
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

        # Create a session-scoped temporary table using the
        # target table's column definitions.
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {TEMP_TABLE}
            LIKE {DATABASE}.{SCHEMA}.{TARGET_TABLE}
        """)

        success, nchunks, nrows, _ = write_pandas(
            connection,
            df,
            table_name=TEMP_TABLE,
            database=DATABASE,
            schema=SCHEMA,
            auto_create_table=False,
            overwrite=False,
        )

        if not success or nrows != len(df):
            raise RuntimeError(
                f"Temporary load failed: success={success}, "
                f"rows_loaded={nrows}, expected={len(df)}"
            )

        print("Temporary rows loaded:", nrows)

        # Update matched transactions only when their values differ.
        update_columns = [
            column for column in COLUMNS
            if column != "TRANSACTION_ID"
        ]

        change_condition = " OR ".join(
            f"target.{column} IS DISTINCT FROM source.{column}"
            for column in update_columns
        )

        update_set = ", ".join(
            f"target.{column} = source.{column}"
            for column in update_columns
        )

        insert_columns = ", ".join(COLUMNS)
        insert_values = ", ".join(
            f"source.{column}" for column in COLUMNS
        )

        merge_sql = f"""
            MERGE INTO {DATABASE}.{SCHEMA}.{TARGET_TABLE} AS target
            USING {TEMP_TABLE} AS source
                ON target.TRANSACTION_ID = source.TRANSACTION_ID

            WHEN MATCHED AND ({change_condition}) THEN
                UPDATE SET {update_set}

            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
        """

        cursor.execute(merge_sql)

        print("Snowflake MERGE completed successfully.")
        print("Rows affected by MERGE:", cursor.rowcount)
        print(
            "The target table was updated without inserting "
            "duplicate transaction IDs."
        )

    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
