from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.fa_chart import fa_chart_update, check_update

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
}

with DAG(
    dag_id="fa_chart_update",
    default_args=default_args,
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 6 * * 1",
    catchup=False,
    tags=["fa_chart", "update"],
) as dag:

    check_update_task = ShortCircuitOperator(
        task_id="check_update",
        python_callable=check_update,
    )

    fa_chart_update_task = PythonOperator(
        task_id="fa_chart_update",
        python_callable=fa_chart_update,
    )

    check_update_task >> fa_chart_update_task