# dag_info_tradingview.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info import sync_tradingview

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="info_sync_tradingview",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 16 * * 1-5",
    catchup=False,
    tags=["info", "asset", "tradingview"]
) as dag:

    sync_tradingview_task = PythonOperator(
        task_id='sync_tradingview',
        python_callable=sync_tradingview
    )

    sync_tradingview_task