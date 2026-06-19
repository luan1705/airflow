from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.breadth.history import adv_dec_indicators

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="breadth_adv_dec_indicators",
    default_args=default_args,
    start_date=datetime(2026, 6, 15, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["exchange_history", "breadth", "adv_dec"],
) as dag:

    adv_dec_indicators_task = PythonOperator(
        task_id="adv_dec",
        python_callable=adv_dec_indicators,
    )

    adv_dec_indicators_task
