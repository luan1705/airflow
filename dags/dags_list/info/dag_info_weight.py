# dag_info_weight.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info import update_market_weight

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="info_update_market_weight",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 5 * * 1",
    catchup=False,
    tags=["info", "asset", "weight"]
) as dag:

    update_market_weight_task = PythonOperator(
        task_id='update_market_weight',
        python_callable=update_market_weight
    )

    update_market_weight_task