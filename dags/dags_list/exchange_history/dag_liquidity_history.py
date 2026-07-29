from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.liquidity_history import liquidity_history

default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="liquidity_history",
    default_args=default_args,
    start_date=datetime(2025, 9, 11, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="30 8 * * 1-5",
    catchup=False,
    tags=["DB", "market_data"]
) as dag:

    save_liquidity_history = PythonOperator(
        task_id='save_liquidity_history',
        python_callable=liquidity_history,
    )