from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.cpi import cpi_mom


with DAG(
    dag_id="vimo_cpi_mom",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "cpi_mom","excel"],
) as dag:

    save_cpi_mom = PythonOperator(
        task_id="cpi_mom",
        python_callable=cpi_mom,
    )
