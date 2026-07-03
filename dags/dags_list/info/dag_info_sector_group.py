# dag_info_sector_group.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info import update_sector_group

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="info_update_sector_group",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 5 1 * *",
    catchup=False,
    tags=["info", "asset", "sector_group"]
) as dag:

    update_sector_group_task = PythonOperator(
        task_id='update_sector_group',
        python_callable=update_sector_group
    )

    update_sector_group_task