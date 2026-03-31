from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.proprietary import main
from utils.details.proprietary import proprietary_HOSE, proprietary_HNX, proprietary_UPCOM
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="exchange_proprietary_history",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="1 0 * * 1-5",
    catchup= False,
    tags=["DB", "market_data"]
) as dag:


    save_proprietary_1D=PythonOperator(
        task_id='save_proprietary_1D',
        python_callable=main
    )
    
    proprietary_HNX=PythonOperator(
        task_id='proprietary_HNX',
        python_callable=proprietary_HNX
    )
    
    proprietary_HOSE=PythonOperator(
        task_id='proprietary_HOSE',
        python_callable=proprietary_HOSE
    )
    
    proprietary_UPCOM=PythonOperator(
        task_id='proprietary_UPCOM',
        python_callable=proprietary_UPCOM
    ) 
    
    save_proprietary_1D >> [proprietary_HNX,proprietary_HOSE,proprietary_UPCOM]