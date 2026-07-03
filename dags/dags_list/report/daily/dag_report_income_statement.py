# dag_income_statement_missing.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.report import income_statement_missing

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="report_income_statement_missing",
    default_args=default_args,
    start_date=datetime(2026, 6, 30, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 5 * * 1",
    catchup=False,
    tags=["report", "income_statement", "missing"]
) as dag:

    income_statement_missing_task = PythonOperator(
        task_id="income_statement_missing",
        python_callable=income_statement_missing,
    )

    income_statement_missing_task