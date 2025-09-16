from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.industry import industry

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="industry",
    default_args=default_args,
    start_date=datetime(2025, 9, 11, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="* * * * *",   # chạy mỗi phút
    catchup=False,
    tags=["DB", "history_data"]
) as dag:

    save_industry = PythonOperator(
        task_id='save_industry',
        python_callable=industry
    )
