from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.cpi import cpi_contribution


with DAG(
    dag_id="vimo_cpi_contribution",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "cpi_contribution"],
) as dag:

    save_cpi_contribution = PythonOperator(
        task_id="cpi_contribution",
        python_callable=cpi_contribution,
    )
