from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.dividend import dividend

default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="dividend",
    default_args=default_args,
    start_date=datetime(2026, 6, 4, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="0 9 * * 1-5",   # chạy mỗi ngày
    catchup=False,
    tags=["dividend", "symbol"]
) as dag:

    save_dividend = PythonOperator(
        task_id='save_dividend',
        python_callable=dividend,
    )