from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.disbursement import disbursement_cal


with DAG(
    dag_id="vimo_disbursement_cal",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "disbursement_cal"],
) as dag:

    save_disbursement_cal = PythonOperator(
        task_id="disbursement_cal",
        python_callable=disbursement_cal,
    )
