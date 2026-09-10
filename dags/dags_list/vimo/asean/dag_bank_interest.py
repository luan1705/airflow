from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.asean import bank_interest

default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="bank_interest",
    default_args=default_args,
    start_date=datetime(2026, 6, 4, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval= "0 6 * * 1-5",
    catchup=False,
    tags=["vimo", "bank_interest"]
) as dag:

    save_bank_interest = PythonOperator(
        task_id="save_bank_interest",
        python_callable=bank_interest,
    )