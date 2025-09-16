from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.market_history.foreign import *
from utils.market_data.foreign import *
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="foreign_1_live",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="* * * * *",
    catchup= False,
    tags=["DB", "market_data"]
) as dag:


    save_foreign_1=PythonOperator(
        task_id='save_foreign_1',
        python_callable=main_1
    )
    
    live_foreign_ALL=PythonOperator(
        task_id='live_foreign_ALL',
        python_callable=foreign_ALL
    )
    
    live_foreign_HNX=PythonOperator(
        task_id='live_foreign_HNX',
        python_callable=foreign_HNX
    )
    
    live_foreign_HOSE=PythonOperator(
        task_id='live_foreign_HOSE',
        python_callable=foreign_HOSE
    )
    
    live_foreign_UPCOM=PythonOperator(
        task_id='live_foreign_UPCOM',
        python_callable=foreign_UPCOM
    )
    
    
    save_foreign_1 >> [live_foreign_ALL,live_foreign_HNX,live_foreign_HOSE,live_foreign_UPCOM]