from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.import_redis_history import run_multithreaded_cache

default_args = {
    "retries": 20,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="import_redis_ohlcv",
    default_args=default_args,
    start_date=datetime(2025,12,19, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="50 8 * * 1-5",   # 08:50 sáng mỗi ngày
    catchup=False,
    tags=["redis", "ohlcv"],
) as dag:

    import_redis = PythonOperator(
        task_id='import_redis',
        python_callable=run_multithreaded_cache,
    )

    import_redis