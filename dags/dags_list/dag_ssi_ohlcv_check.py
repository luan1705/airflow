from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.ssi_ohlcv_check import tradingview_1D
default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="ssi_ohlcv_check",
    default_args=default_args,
    start_date=datetime(2026,5,5,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 6 * * 1-5",
    catchup= False,
    tags=["DB", "ETL"]
) as dag:

    tradingview_1D=PythonOperator(
        task_id='tradingview_1D',
        python_callable=tradingview_1D
    )


    tradingview_1D