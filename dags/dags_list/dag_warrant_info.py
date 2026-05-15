from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.warrant import warrant_info

default_args = {
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="warrant_info",
    default_args=default_args,
    start_date=datetime(2026, 4, 10, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="0 1 1 * *",   # chạy mỗi phút trong giờ giao dịch
    catchup=False,
    tags=["DB", "info", "warrant"],
) as dag:

    save_warrant_info = PythonOperator(
        task_id='save_warrant_info',
        python_callable=warrant_info,
    )