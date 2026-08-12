# dag_balance_sheet_update.py
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.report import balance_sheet_update, check_update

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="report_balance_sheet_update",
    default_args=default_args,
    start_date=datetime(2026, 6, 30, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 5 * * 1",
    catchup=False,
    tags=["report", "balance_sheet", "update"]
) as dag:

    check_update_task = ShortCircuitOperator(
        task_id="check_update",
        python_callable=check_update,
        op_kwargs={
            "schema": "balance_sheet",
        },
    )

    balance_sheet_update_task = PythonOperator(
        task_id="balance_sheet_update",
        python_callable=balance_sheet_update,
    )

    check_update_task >> balance_sheet_update_task