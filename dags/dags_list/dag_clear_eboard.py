from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.clear_eboard import clear_eboard

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="clear_eboard",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="1 9 * * 1-5",
    catchup= False,
    tags=["DB", "clear_eboard"]
) as dag:

    clear_eboard=PythonOperator(
        task_id='clear_eboard',
        python_callable=clear_eboard
    )


    clear_eboard