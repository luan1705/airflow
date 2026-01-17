from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.eboard import save_cq

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="saveeboardcq",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 * * * *",
    catchup= False,
    tags=["DB", "eboard"]
) as dag:

    save_eboard_cq=PythonOperator(
        task_id='save_cq',
        python_callable=save_cq
    )


    save_eboard_cq