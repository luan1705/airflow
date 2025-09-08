from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình mặc định
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="crypto_collector_dag",
    default_args=default_args,
    description="Thu thập dữ liệu crypto từ Binance mỗi phút",
    schedule_interval="*/1 * * * *",   # chạy mỗi phút
    start_date=datetime(2025, 8, 17),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "binance"],
) as dag:

    run_collector = BashOperator(
        task_id="run_crypto_collector",
        bash_command="python3 /opt/airflow/dags/crawl_crypto/crawl_data_crypto.py"
    )

    run_collector
