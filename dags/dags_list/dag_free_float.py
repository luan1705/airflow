from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.free_float import free_float

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="free_float",
    default_args=default_args,
    start_date=datetime(2026, 5, 25, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["DB", "ETL"]
) as dag:

    get_free_float = PythonOperator(
        task_id='free_float',
        python_callable=free_float
    )