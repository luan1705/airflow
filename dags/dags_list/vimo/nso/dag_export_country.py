from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.export_country import export_country, export_country_cal


with DAG(
    dag_id="vimo_export_country",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "export_country","word"],
) as dag:

    save_export_country = PythonOperator(
        task_id="export_country",
        python_callable=export_country,
    )

    cal_export_country = PythonOperator(
        task_id="export_country_cal",
        python_callable=export_country_cal,
    )

    save_export_country >> cal_export_country
