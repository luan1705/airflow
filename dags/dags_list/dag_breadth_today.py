from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.breadth.breadth_today import breadth_today

default_args = {
    "retries": 20,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="breadth_today",
    default_args=default_args,
    start_date=datetime(2025, 9, 17, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 15 * * 1-5",
    catchup=False,
    tags=["DB", "market_data"],
) as dag:

    save_breadth_today = PythonOperator(
        task_id="save_breadth_today",
        python_callable=breadth_today,
    )

    save_breadth_today
