from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.asean import exchange_rate

default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="exchange_rate",
    default_args=default_args,
    start_date=datetime(2026, 6, 4, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval= "0 6 * * 1-5",
    catchup=False,
    tags=["vimo", "exchange_rate"]
) as dag:

    save_exchange_rate = PythonOperator(
        task_id="save_exchange_rate",
        python_callable=exchange_rate,
    )