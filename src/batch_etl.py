from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)



# 1. Project paths


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_SALES_PATH = str(
    PROJECT_ROOT / "data" / "raw" / "sales" / "sales_*.csv"
)

PROCESSED_PATH = str(
    PROJECT_ROOT / "data" / "processed" / "sales"
)

REJECTED_PATH = str(
    PROJECT_ROOT / "data" / "rejected" / "sales"
)



# 2. Define the expected input schema


SALES_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("unit_cost", StringType(), True),
        StructField("sales_amount", StringType(), True),
        StructField("cost_amount", StringType(), True),
        StructField("profit_amount", StringType(), True),
    ]
)



# 3. Start Spark


def main():
    spark = (
        SparkSession.builder
        .appName("RetailWarehouseBatchETL")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\nStarting Retail Warehouse batch ETL...")

       
        # 4. Read monthly sales CSV files

        raw_df = (
            spark.read
            .option("header", True)
            .schema(SALES_SCHEMA)
            .csv(RAW_SALES_PATH)
        )

        input_count = raw_df.count()
        print(f"Input records: {input_count:,}")

        # 5. Convert data types

        df = (
            raw_df
            .withColumn(
                "transaction_date",
                F.to_date("transaction_date", "yyyy-MM-dd"),
            )
            .withColumn(
                "quantity",
                F.col("quantity").cast(IntegerType()),
            )
            .withColumn(
                "unit_price",
                F.col("unit_price").cast(DecimalType(12, 2)),
            )
            .withColumn(
                "unit_cost",
                F.col("unit_cost").cast(DecimalType(12, 2)),
            )
            .withColumn(
                "sales_amount",
                F.col("sales_amount").cast(DecimalType(14, 2)),
            )
            .withColumn(
                "cost_amount",
                F.col("cost_amount").cast(DecimalType(14, 2)),
            )
            .withColumn(
                "profit_amount",
                F.col("profit_amount").cast(DecimalType(14, 2)),
            )
        )

        # 6. Define data-quality rules

        required_columns = [
            "transaction_id",
            "transaction_date",
            "product_id",
            "customer_id",
            "store_id",
            "quantity",
            "payment_method",
            "order_status",
            "unit_price",
            "unit_cost",
            "sales_amount",
            "cost_amount",
            "profit_amount",
        ]

        has_missing_required = F.lit(False)

        for column_name in required_columns:
            has_missing_required = (
                has_missing_required
                | F.col(column_name).isNull()
                | (F.trim(F.col(column_name).cast("string")) == "")
            )

        invalid_condition = (
            has_missing_required
            | (F.col("quantity") <= 0)
            | (F.col("unit_price") < 0)
            | (F.col("unit_cost") < 0)
            | (
                ~F.col("order_status").isin(
                    "Completed", "Returned"
                )
            )
            | (
                ~F.col("payment_method").isin(
                    "Cash", "Card", "UPI", "NetBanking"
                )
            )
            | (
                F.col("sales_amount")
                != (
                    F.col("quantity") * F.col("unit_price")
                ).cast(DecimalType(14, 2))
            )
            | (
                F.col("cost_amount")
                != (
                    F.col("quantity") * F.col("unit_cost")
                ).cast(DecimalType(14, 2))
            )
            | (
                F.col("profit_amount")
                != (
                    F.col("sales_amount") - F.col("cost_amount")
                ).cast(DecimalType(14, 2))
            )
        )

        # 7. Separate invalid records

        rejected_df = df.filter(invalid_condition)

        rejected_count = rejected_df.count()

        (
            rejected_df
            .withColumn("rejection_reason", F.lit("Failed data-quality rule"))
            .write
            .mode("overwrite")
            .parquet(REJECTED_PATH)
        )

        # 8. Keep valid records and remove duplicates

        valid_df = df.filter(~invalid_condition)

        duplicate_count = (
            valid_df.count()
            - valid_df.dropDuplicates(["transaction_id"]).count()
        )

        clean_df = valid_df.dropDuplicates(["transaction_id"])

        # 9. Write clean data in Parquet format

        (
            clean_df
            .write
            .mode("overwrite")
            .parquet(PROCESSED_PATH)
        )

        # 10. Print the ETL summary

        output_count = clean_df.count()

        print("\n========== ETL SUMMARY ==========")
        print(f"Input records:     {input_count:,}")
        print(f"Rejected records:  {rejected_count:,}")
        print(f"Duplicate records: {duplicate_count:,}")
        print(f"Output records:    {output_count:,}")
        print(f"Processed path:    {PROCESSED_PATH}")
        print(f"Rejected path:     {REJECTED_PATH}")
        print("ETL completed successfully.")
        print("=================================\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()