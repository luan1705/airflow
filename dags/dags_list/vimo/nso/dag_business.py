from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.business import business, business_cal


with DAG(
    dag_id="vimo_business",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "business","excel"],
) as dag:

    save_business = PythonOperator(
        task_id="business",
        python_callable=business,
    )

    cal_business_cal = PythonOperator(
        task_id="business_cal",
        python_callable=business_cal,
    )

    save_business >> cal_business_cal
