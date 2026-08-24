from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.impact import impact_history_full

default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="impact_history_full",
    default_args=default_args,
    start_date=datetime(2025, 9, 11, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval=None,
    catchup=False,
    tags=["exchange_history", "impact_history_full"]
) as dag:

    save_impact_history_full = PythonOperator(
        task_id='save_impact_history_full',
        python_callable=impact_history_full,
    )