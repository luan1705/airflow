from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.ssi_ohlcv import tradingview_1D,tradingview_1

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="ssi_ohlcv",
    default_args=default_args,
    start_date=datetime(2025,12,18,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="15 15 * * 1-5",
    catchup= False,
    tags=["ohlcv", "asset", "ssi"],
) as dag:

    tradingview_1D=PythonOperator(
        task_id='tradingview_1D',
        python_callable=tradingview_1D
    )

    tradingview_1=PythonOperator(
        task_id='tradingview_1',
        python_callable=tradingview_1
    )

    (tradingview_1D,tradingview_1)