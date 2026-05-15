from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.pepb import main

default_args = {
    "retries": 20,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="pepb",
    default_args=default_args,
    start_date=datetime(2025, 9, 17, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 1 * * 1-5",
    catchup=False,
    tags=["DB", "market_data"],
) as dag:

    save_pepb = PythonOperator(
        task_id="save_pepb",
        python_callable=main,
    )

    save_pepb
