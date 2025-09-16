from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.foreign_history import save_all_foreign

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="save_all_foreign",
    default_args=default_args,
    start_date=datetime(2025,9,12,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 1 * * *",
    catchup= False,
    tags=["DB", "foreign_symbol"]
) as dag:

    foreign_history=PythonOperator(
        task_id='save_foreign_history',
        python_callable=save_all_foreign
    )


    foreign_history