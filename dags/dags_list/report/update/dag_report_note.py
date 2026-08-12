# dag_note_update.py
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.report import note_update, check_update

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="report_note_update",
    default_args=default_args,
    start_date=datetime(2026, 6, 30, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 4 * * 1",
    catchup=False,
    tags=["report", "note", "update"]
) as dag:

    check_update_task = ShortCircuitOperator(
        task_id="check_update",
        python_callable=check_update,
        op_kwargs={
            "schema": "note",
        },
    )

    note_update_task = PythonOperator(
        task_id="note_update",
        python_callable=note_update,
    )

    check_update_task >> note_update_task