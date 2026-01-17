from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.import_redis_history import run_multithreaded_cache
from utils.import_redis_history.List import generate_symbol_list

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="import_redis_ohlcv",
    default_args=default_args,
    start_date=datetime(2025,12,19,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="50 8 * * 1-5",
    catchup= False,
    tags=["redis", "redis", "ohlcv"],
) as dag:
    update_symbol_list=PythonOperator(
        task_id='update_symbol_list',
        python_callable=generate_symbol_list
    )

    import_redis=PythonOperator(
        task_id='import_redis',
        python_callable=run_multithreaded_cache
    )

    update_symbol_list >> import_redis