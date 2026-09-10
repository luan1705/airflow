from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.import_country import import_country, import_country_cal


with DAG(
    dag_id="vimo_import_country",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "import_country","word"],
) as dag:

    save_import_country = PythonOperator(
        task_id="import_country",
        python_callable=import_country,
    )

    cal_import_country = PythonOperator(
        task_id="import_country_cal",
        python_callable=import_country_cal,
    )

    save_import_country >> cal_import_country