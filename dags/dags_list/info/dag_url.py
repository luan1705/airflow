from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.info import url

default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="url",
    default_args=default_args,
    start_date=datetime(2026, 6, 28, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval= None,   # chạy mỗi phút
    catchup=False,
    tags=["info", "asset", "url"]
) as dag:

    save_url = PythonOperator(
        task_id='save_url',
        python_callable=url,
    )