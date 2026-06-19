from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.asset_foreign_history import save_all_foreign_1D

default_args = {
    "retries": 20,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="asset_foreign_history_1D",
    default_args=default_args,
    start_date=datetime(2025,9,17,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="1 9 * * 1-5",
    catchup=False,
    tags=["DB", "foreign_asset"],
) as dag:

    asset_foreign_history_1D = PythonOperator(
        task_id='foreign_history_1D',
        python_callable=save_all_foreign_1D,
    )

    asset_foreign_history_1D