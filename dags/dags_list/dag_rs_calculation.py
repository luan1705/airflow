from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.relative_strength import etl_rs

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="rs_calculation",
    default_args=default_args,
    start_date=datetime(2026, 5, 25, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["DB", "ETL"]
) as dag:

    calc_rs_task = PythonOperator(
        task_id='calc_rs',
        python_callable=etl_rs
    )