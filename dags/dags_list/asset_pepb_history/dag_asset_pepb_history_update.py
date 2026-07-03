from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.asset_pepb_history import asset_pepb_history_update

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="asset_pepb_history_update",
    default_args=default_args,
    start_date=datetime(2025, 9, 17, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 9 * * 1-5",
    catchup=False,
    tags=["asset_pepb_history", "pepb", "update"],
) as dag:

    update_asset_pepb_history = PythonOperator(
        task_id="asset_pepb_history_update",
        python_callable=asset_pepb_history_update,
    )

    update_asset_pepb_history
