from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.market_history.foreign import *
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="foreign_1D",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="1 0 * * *",
    catchup= True,
    tags=["DB", "market_data"]
) as dag:


    save_foreign_1D=PythonOperator(
        task_id='save_foreign_1D',
        python_callable=main_1D
    )
    
    save_foreign_1D