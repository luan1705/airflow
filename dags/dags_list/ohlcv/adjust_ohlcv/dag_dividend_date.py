from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.adjust_ohlcv.dividend_date import save_dividend_date

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id="dividend_date",
    default_args=default_args,
    start_date=datetime(2026, 5, 5, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 6 * * 1-5",
    catchup=False,
    tags=["ohlcv_check", "adjust_ohlcv"]
) as dag:

    save_dividend_date = PythonOperator(
        task_id="save_dividend_date",
        python_callable=save_dividend_date,
        do_xcom_push=False,
    )

    save_dividend_date