from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.relative_strength import rs_rank

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="rs_rank",
    default_args=default_args,
    start_date=datetime(2026, 5, 25, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["DB", "ETL"]
) as dag:

    rs_rank_task = PythonOperator(
    task_id='calc_rs_rank',
    python_callable=rs_rank
)