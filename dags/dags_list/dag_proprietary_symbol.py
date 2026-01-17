from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.proprietary_history_symbol import save_proprietary_history

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="save_proprietary_symbol_1D",
    default_args=default_args,
    start_date=datetime(2025,9,18,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 1 * * 1-5",
    catchup= False,
    tags=["DB", "proprietary_symbol"]
) as dag:

    proprietary_symbol_1D=PythonOperator(
        task_id='proprietary_history_1D',
        python_callable=save_proprietary_history
    )


    proprietary_symbol_1D