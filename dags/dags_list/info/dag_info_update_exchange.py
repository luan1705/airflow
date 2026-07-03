# dag_info_update_exchange.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info import update_exchange

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="info_update_exchange",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 16 * * 1-5",
    catchup=False,
    tags=["info", "asset", "exchange"]
) as dag:

    update_exchange_task = PythonOperator(
        task_id='update_exchange',
        python_callable=update_exchange
    )

    update_exchange_task