# dag_income_statement_update.py
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.report import income_statement_update, check_update

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="report_income_statement_update",
    default_args=default_args,
    start_date=datetime(2026, 6, 30, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 7 * * 1",
    catchup=False,
    tags=["report", "income_statement", "update"]
) as dag:

    check_update_task = ShortCircuitOperator(
        task_id="check_update",
        python_callable=check_update,
        op_kwargs={
            "schema": "income_statement",
        },
    )

    income_statement_update_task = PythonOperator(
        task_id="income_statement_update",
        python_callable=income_statement_update,
    )

    check_update_task >> income_statement_update_task