from pathlib import Path

import boto3
from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BUCKET_NAME = "realtime-ecommerce-raw-amitchaudhary-2026"
S3_PREFIX = "retail-analytics-warehouse"
AWS_REGION = "ap-southeast-2"


def upload_directory(s3_client, local_directory, s3_prefix):
    """Upload all files in a local directory while preserving its structure."""
    local_directory = Path(local_directory)

    if not local_directory.exists():
        print(f"Directory not found; skipping: {local_directory}")
        return

    for file_path in local_directory.rglob("*"):
        if not file_path.is_file():
            continue

        relative_path = file_path.relative_to(local_directory).as_posix()
        s3_key = f"{s3_prefix}/{relative_path}"

        try:
            s3_client.upload_file(
                str(file_path),
                BUCKET_NAME,
                s3_key,
            )
            print(f"UPLOADED: s3://{BUCKET_NAME}/{s3_key}")

        except ClientError as error:
            print(f"FAILED: {file_path}")
            print(error)
            raise


def main():
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # Upload original CSV files.
    upload_directory(
        s3_client,
        PROJECT_ROOT / "data" / "raw",
        f"{S3_PREFIX}/raw",
    )

    # Upload cleaned Parquet files.
    upload_directory(
        s3_client,
        PROJECT_ROOT / "data" / "processed" / "sales",
        f"{S3_PREFIX}/processed/sales",
    )

    print("\nS3 upload process finished.")


if __name__ == "__main__":
    main()
