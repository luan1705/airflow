from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.insider_tranansaction import save_all_pg

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="insider_transaction",
    default_args=default_args,
    start_date=datetime(2025,12,18,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="15 15 * * 1-5",
    catchup= False,
    tags=["DB", "ETL"]
) as dag:

    save_insider_transaction=PythonOperator(
        task_id='save_insider_transaction',
        python_callable=save_all_pg
    )

    save_insider_transaction