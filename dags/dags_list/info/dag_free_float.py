from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.info import free_float

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="info_free_float",
    default_args=default_args,
    start_date=datetime(2026, 5, 25, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 5 * * 1",
    catchup=False,
    tags=["info", "asset", "free_float"]
) as dag:

    get_free_float = PythonOperator(
        task_id='free_float',
        python_callable=free_float
    )