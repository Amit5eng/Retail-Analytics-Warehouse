from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/retail_project"
DBT_DIR = f"{PROJECT_DIR}/dbt"
PROFILES_DIR = f"{PROJECT_DIR}/config"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
}

with DAG(
    dag_id="retail_warehouse",
    default_args=default_args,
    description="Retail Analytics Warehouse end-to-end data pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["retail", "data-engineering", "dbt"],
) as dag:

    batch_etl = BashOperator(
        task_id="batch_etl",
        bash_command=f"cd {PROJECT_DIR} && python src/batch_etl.py",
    )

    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"cd {PROJECT_DIR} && python src/s3_uploader.py",
    )

    load_master_data = BashOperator(
        task_id="load_master_data",
        bash_command=f"cd {PROJECT_DIR} && python src/load_master_data.py",
    )

    load_sales_to_snowflake = BashOperator(
        task_id="load_sales_to_snowflake",
        bash_command=f"cd {PROJECT_DIR} && python src/snowflake_loader.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --project-dir {DBT_DIR} "
            f"--profiles-dir {PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_DIR} "
            f"--profiles-dir {PROFILES_DIR}"
        ),
    )

    (
        batch_etl
        >> upload_to_s3
        >> load_master_data
        >> load_sales_to_snowflake
        >> dbt_run
        >> dbt_test
    )
