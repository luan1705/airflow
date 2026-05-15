from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.breadth import main

default_args = {
    "retries": 20,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="breadth",
    default_args=default_args,
    start_date=datetime(2025, 9, 17, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="16 9 * * 1-5",
    catchup=False,
    tags=["DB", "market_data"],
) as dag:

    save_breadth = PythonOperator(
        task_id="save_breadth",
        python_callable=main,
    )

    save_breadth
