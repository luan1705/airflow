from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.olchv_indices import save_olch

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="upsert_olchv_indices",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="*/5 9-15 * * 1-5",
    catchup= False,
    tags=["dashboard", "indices", "olchv"],
    max_active_runs=1
) as dag:

    upsert_olchv_indices=PythonOperator(
        task_id='upsert_olchv_indices',
        python_callable=save_olch
    )

    upsert_olchv_indices