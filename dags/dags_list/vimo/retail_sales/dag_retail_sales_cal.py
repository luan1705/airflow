from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.retail_sales import retail_sales_cal


with DAG(
    dag_id="vimo_retail_sales_cal",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "retail_sales_cal"],
) as dag:

    save_retail_sales = PythonOperator(
        task_id="retail_sales_cal",
        python_callable=retail_sales_cal,
    )
