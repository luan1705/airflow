# dag_info_eps.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info import fetch_eps_all

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="info_fetch_eps",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 16 * * 1-5",
    catchup=False,
    tags=["info", "asset", "eps"]
) as dag:

    fetch_eps_all_task = PythonOperator(
        task_id='fetch_eps_all',
        python_callable=fetch_eps_all
    )

    fetch_eps_all_task