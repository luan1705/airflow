from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.liquidity import main

default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="liquidity",
    default_args=default_args,
    start_date=datetime(2025, 9, 11, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="* 9-15 * * 1-5",   # chạy mỗi phút
    catchup=False,
    tags=["DB", "market_data"]
) as dag:

    save_liquidity = PythonOperator(
        task_id='save_liquidity',
        python_callable=main,
    )