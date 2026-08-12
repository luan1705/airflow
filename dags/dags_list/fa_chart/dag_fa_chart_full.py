from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.fa_chart import fa_chart_history

with DAG(
    dag_id="fa_chart_history",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,  # chạy thủ công
    catchup=False,
    tags=["fa_chart", "full_history"],
) as dag:

    PythonOperator(
        task_id="fa_chart_history",
        python_callable=fa_chart_history,
    )