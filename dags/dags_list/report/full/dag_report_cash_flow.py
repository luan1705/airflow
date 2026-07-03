# dag_cash_flow_full.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.report import cash_flow_full

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="report_cash_flow_full",
    default_args=default_args,
    start_date=datetime(2026, 6, 30, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["report", "cash_flow", "full"]
) as dag:

    cash_flow_full_task = PythonOperator(
        task_id="cash_flow_full",
        python_callable=cash_flow_full,
    )

    cash_flow_full_task