from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.sector_cmv_history import sector_cmv_history_all

default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="sector_cmv_history",
    default_args=default_args,
    start_date=datetime(2026, 6, 4, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="50 8 * * 1-5",   # chạy mỗi ngày
    catchup=False,
    tags=["exchange_history", "sector_cmv_history"]
) as dag:

    save_sector_cmv_history = PythonOperator(
        task_id='save_sector_cmv_history',
        python_callable=sector_cmv_history_all,
    )