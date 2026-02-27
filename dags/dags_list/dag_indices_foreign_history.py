from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.indices_foreign_history import run_all_indices 

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="indices_foreign_history",
    default_args=default_args,
    start_date=datetime(2025,12,2,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 2 * * 1-5",
    catchup= False,
    tags=["DB", "ETL"]
) as dag:

    run_indices_foreign_history=PythonOperator(
        task_id='run_indices_foreign_history',
        python_callable=run_all_indices
    )

    run_indices_foreign_history