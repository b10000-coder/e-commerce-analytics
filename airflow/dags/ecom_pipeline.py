from datetime import datetime, timedelta
import subprocess
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT_ROOT = "/Users/burakkurt/data_engineering_projects/e-commerce-analytics"
DBT_VENV = PROJECT_ROOT + "/.venv/bin"
DBT_DIR = PROJECT_ROOT + "/ecom"
RAW_DIR = PROJECT_ROOT + "/data/raw"

default_args = {
    "owner": "burak",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

def check_freshness(**context):
    import time
    orders_path = RAW_DIR + "/orders.csv"
    if not os.path.exists(orders_path):
        print("STALE: orders.csv not found")
        return "notify_stale"
    age_hours = (time.time() - os.path.getmtime(orders_path)) / 3600
    print("orders.csv age: %.1f hours" % age_hours)
    if age_hours > 24:
        print("STALE: skipping build")
        return "notify_stale"
    print("FRESH: proceeding with build")
    return "load_raw_to_bigquery"

def load_raw(**context):
    result = subprocess.run(
        [PROJECT_ROOT + "/.venv/bin/python",
         PROJECT_ROOT + "/scripts/load_bigquery.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception("load_bigquery.py failed: " + result.stderr)

def run_dbt(**context):
    result = subprocess.run(
        [DBT_VENV + "/dbt", "build",
         "--profiles-dir", ".",
         "--target", "bigquery"],
        capture_output=True, text=True,
        cwd=DBT_DIR
    )
    print(result.stdout[-3000:])
    if result.returncode != 0:
        raise Exception("dbt build failed: " + result.stderr)

with DAG(
    dag_id="ecom_daily_pipeline",
    default_args=default_args,
    description="Daily ecom pipeline: freshness check, BQ load, dbt build",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 10, 1),
    catchup=False,
    tags=["ecom", "dbt", "bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")

    freshness_check = BranchPythonOperator(
        task_id="check_source_freshness",
        python_callable=check_freshness,
    )

    load_raw_task = PythonOperator(
        task_id="load_raw_to_bigquery",
        python_callable=load_raw,
    )

    dbt_build = PythonOperator(
        task_id="dbt_build_bigquery",
        python_callable=run_dbt,
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "Pipeline complete"',
        trigger_rule="none_failed_min_one_success",
    )

    notify_stale = BashOperator(
        task_id="notify_stale",
        bash_command='echo "SKIPPED: source data stale"',
    )

    start >> freshness_check
    freshness_check >> load_raw_task >> dbt_build >> notify_success
    freshness_check >> notify_stale
