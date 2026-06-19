from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.asset_proprietary_history import save_proprietary_history

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="asset_proprietary_history",
    default_args=default_args,
    start_date=datetime(2025,9,18,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 9 * * 1-5",
    catchup= False,
    tags=["DB", "proprietary_symbol"]
) as dag:

    asset_proprietary_history=PythonOperator(
        task_id='asset_proprietary_history',
        python_callable=save_proprietary_history
    )


    asset_proprietary_history