# dag_balance_sheet_missing.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.report import balance_sheet_missing

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="report_balance_sheet_missing",
    default_args=default_args,
    start_date=datetime(2026, 6, 30, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 5 * * 1",
    catchup=False,
    tags=["report", "balance_sheet", "missing"]
) as dag:

    balance_sheet_missing_task = PythonOperator(
        task_id="balance_sheet_missing",
        python_callable=balance_sheet_missing,
    )

    balance_sheet_missing_task