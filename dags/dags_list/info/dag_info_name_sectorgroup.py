# dag_info_name_sectorgroup.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info import info_name_sectorgroup

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="info_name_sectorgroup",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 5 * * 1",
    catchup=False,
    tags=["info", "asset", "company"]
) as dag:

    info_name_sectorgroup_task = PythonOperator(
        task_id='info_name_sectorgroup',
        python_callable=info_name_sectorgroup
    )

    info_name_sectorgroup_task