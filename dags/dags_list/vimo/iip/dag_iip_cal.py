from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.iip import iip_cal


with DAG(
    dag_id="vimo_iip_cal",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "iip_cal"],
) as dag:

    save_iip = PythonOperator(
        task_id="iip_cal",
        python_callable=iip_cal,
    )
