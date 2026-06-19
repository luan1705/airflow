from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.breadth.history import pepb_breadth

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="breadth_pepb",
    default_args=default_args,
    start_date=datetime(2026, 6, 18, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["exchange_history", "breadth", "pepb"],
) as dag:

    pepb_breadth_task = PythonOperator(
        task_id="pepb_breadth",
        python_callable=pepb_breadth,
    )

    pepb_breadth_task
