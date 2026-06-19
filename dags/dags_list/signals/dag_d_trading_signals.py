from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.signals import save_all_d_trading_pg

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="d_trading_signals",
    default_args=default_args,
    start_date=datetime(2025,12,18,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 9,10,11,13,14 * * 1-5",
    catchup= False,
    tags=["DB", "ETL"]
) as dag:

    save_d_trading_signals=PythonOperator(
        task_id='save_d_trading_signals',
        python_callable=save_all_d_trading_pg
    )

    save_d_trading_signals