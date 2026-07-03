from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.volatility_history import upsert_volatility_history

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

local_tz = timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="volatility_history_dag",
    default_args=default_args,
    start_date=datetime(2025, 3, 17, tzinfo=local_tz),
    schedule="30 15 * * 1-5",
    catchup=False,
    tags=["DB", "ETL"],
) as dag:

    upsert_volatility_history_task = PythonOperator(
        task_id="upsert_volatility_history",
        python_callable=upsert_volatility_history,
    )

    upsert_volatility_history_task