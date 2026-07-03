from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info.bvps import update_all_bvps

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="info_asset_bvps",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="15 15 * * 1-5",
    catchup=False,
    tags=["info", "asset", "bvps"]
) as dag:

    update_bvps_task = PythonOperator(
        task_id='update_all_bvps',
        python_callable=update_all_bvps
    )

    update_bvps_task