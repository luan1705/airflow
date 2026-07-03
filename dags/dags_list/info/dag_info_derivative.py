# dag_info_derivative.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info import fetch_derivatives_futures

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="info_fetch_derivatives_futures",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 5 * * 1",
    catchup=False,
    tags=["info", "asset", "derivative"]
) as dag:

    fetch_derivatives_futures_task = PythonOperator(
        task_id='fetch_derivatives_futures',
        python_callable=fetch_derivatives_futures
    )

    fetch_derivatives_futures_task