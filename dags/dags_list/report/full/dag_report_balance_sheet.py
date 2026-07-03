from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.report import balance_sheet_full

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="report_balance_sheet_full",
    default_args=default_args,
    start_date=datetime(2026, 6, 30, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["report", "balance_sheet", "full"]
) as dag:

    balance_sheet_full_task = PythonOperator(
        task_id="balance_sheet_full",
        python_callable=balance_sheet_full,
    )

    balance_sheet_full_task