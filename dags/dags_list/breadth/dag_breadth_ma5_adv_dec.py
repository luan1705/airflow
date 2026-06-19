from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.exchange_history.breadth.history import ma5_adv_dec

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="breadth_ma5_adv_dec",
    default_args=default_args,
    start_date=datetime(2026, 6, 15, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["exchange_history", "breadth", "ma5_adv_dec"],
) as dag:

    ma5_adv_dec_task = PythonOperator(
        task_id="ma5_adv_dec",
        python_callable=ma5_adv_dec,
    )

    ma5_adv_dec_task
