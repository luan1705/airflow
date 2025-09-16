from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.stock_event import save_stock_event

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="save_stock_event",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 1 * * *",
    catchup= False,
    tags=["DB", "stock_event"]
) as dag:

    stock_event=PythonOperator(
        task_id='save_stock_event',
        python_callable=save_stock_event
    )


    stock_event