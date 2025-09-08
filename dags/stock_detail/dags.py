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
    dag_id="stock_detail_dag",
    default_args=default_args,
    description="Thu thập dữ liệu stock từ fireant mỗi phút (T2-T6, 9h-15h)",
    schedule_interval="*/1 9-15 * * 1-5",   # mỗi phút từ 9h đến 15h, T2->T6
    start_date=datetime(2025, 8, 28),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "fireant"],
) as dag:

    run_collector = BashOperator(
        task_id="run_stock_details",
        bash_command="python3 /opt/airflow/dags/stock_detail/stockdetail.py"
    )

    run_collector
