from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.transport_tourism import transport_tourism


with DAG(
    dag_id="vimo_transport_tourism",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "transport_tourism","excel"],
) as dag:

    save_transport_tourism = PythonOperator(
        task_id="transport_tourism",
        python_callable=transport_tourism,
    )
