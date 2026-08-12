from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.pepb import pepb_mean_sd

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="exchange_pepb_mean_sd",
    default_args=default_args,
    start_date=datetime(2025, 9, 17, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="3 9 * * 1-5",
    catchup=False,
    tags=["exchange_history", "exchange_pepb","pepb_mean_sd"],
) as dag:

    pepb_mean_sd = PythonOperator(
        task_id="pepb_mean_sd",
        python_callable=pepb_mean_sd,
    )

    pepb_mean_sd
