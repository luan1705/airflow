from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.invest_capital import invest_capital_full

default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="invest_capital_full",
    default_args=default_args,
    start_date=datetime(2026, 6, 4, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval= None,  #"30 17 * * 1-5",   # chạy mỗi ngày
    catchup=False,
    tags=["exchange_history", "invest_capital_full","sstock"]
) as dag:

    save_invest_capital_full = PythonOperator(
        task_id='save_invest_capital_full',
        python_callable=invest_capital_full,
    )