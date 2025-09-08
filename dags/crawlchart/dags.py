from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="update_stock_history",
    default_args=default_args,
    description="Crawl stock history and save to PostgreSQL",
    schedule_interval="*/1 9-15 * * 1-5",  # mỗi phút từ 9h-15h, thứ 2-6
    start_date=datetime(2025, 8, 17),
    catchup=False,
    tags=["stock", "postgres", "crawl"],
) as dag:

    run_get = BashOperator(
        task_id="run_get_py",
        bash_command="python3 /opt/airflow/dags/crawlchart/get.py",
    )

    run_get
