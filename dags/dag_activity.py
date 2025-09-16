from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.market_data.activity import *
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="activity",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="* * * * *",
    catchup= False,
    tags=["DB", "market_data"]
) as dag:


    activity_ALL=PythonOperator(
        task_id='activity_ALL',
        python_callable=activity_ALL
    )
    activity_HOSE=PythonOperator(
        task_id='activity_HOSE',
        python_callable=activity_HOSE
    )
    activity_HNX=PythonOperator(
        task_id='activity_HNX',
        python_callable=activity_HNX
    )
    activity_UPCOM=PythonOperator(
        task_id='activity_UPCOM',
        python_callable=activity_UPCOM
    )
    
    [activity_ALL,activity_HOSE,activity_HNX,activity_UPCOM]