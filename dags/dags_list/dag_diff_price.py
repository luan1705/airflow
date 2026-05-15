from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from utils.diff_price import diff_price

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="diff_price",
    default_args=default_args,
    start_date=datetime(2026,5,5,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="15 8 * * 1-5",
    catchup= False,
    tags=["DB", "CHECK", "DIFF"],
) as dag:

    compare_diff_price = PythonOperator(
        task_id="diff_price",
        python_callable=diff_price,
        do_xcom_push=False,
    )

    compare_diff_price