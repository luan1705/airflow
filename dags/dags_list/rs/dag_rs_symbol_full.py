from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.relative_strength.asset import etl_rs, rs_rank

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="rs_symbol_full",
    default_args=default_args,
    start_date=datetime(2026, 5, 25, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["DB", "ETL"]
) as dag:

    task_rs = PythonOperator(
        task_id='rs_symbol',
        python_callable=etl_rs
    )

    task_rs_rank = PythonOperator(
        task_id='rs_rank_symbol',
        python_callable=rs_rank
    )

    task_rs >> task_rs_rank