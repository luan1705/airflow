from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.foreign import main_1D

default_args = {
    "retries": 20,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="foreign_exchange_1D",
    default_args=default_args,
    start_date=datetime(2025, 9, 17, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 1 * * 1-5",
    catchup=False,
    tags=["DB", "market_data"],
) as dag:

    save_foreign_exchange_1D = PythonOperator(
        task_id="save_foreign_1D",
        python_callable=main_1D,
    )

    save_foreign_exchange_1D