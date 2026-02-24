from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.industry import industry

default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="industry",
    default_args=default_args,
    start_date=datetime(2025, 10, 20, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="* 9-15 * * 1-5",   # chạy mỗi phút
    catchup=False,
    tags=["DB", "details"]
) as dag:

    save_industry = PythonOperator(
        task_id='save_industry',
        python_callable=industry,
    )