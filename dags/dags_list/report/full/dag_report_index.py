# dag_index_full.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.report import index_full

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="report_index_full",
    default_args=default_args,
    start_date=datetime(2026, 6, 30, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["report", "index", "full"]
) as dag:

    index_full_task = PythonOperator(
        task_id="index_full",
        python_callable=index_full,
    )

    index_full_task