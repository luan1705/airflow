from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.adjust_ohlcv.price_diff import save_ohlcv_check
from utils.adjust_ohlcv.price_diff import check_price_diff


default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="check_price_diff",
    default_args=default_args,
    start_date=datetime(2026,5,5,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 6 * * 1-5",
    catchup= False,
    tags=["ohlcv_check", "adjust_ohlcv"]
) as dag:

    save_ohlcv_check=PythonOperator(
        task_id='save_ohlcv_check',
        python_callable=save_ohlcv_check
    )

    check_price_diff = PythonOperator(
        task_id="check_price_diff",
        python_callable=check_price_diff,
        do_xcom_push=False,
    )


    save_ohlcv_check >> check_price_diff