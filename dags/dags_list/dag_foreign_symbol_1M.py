from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.asset_history_symbol import save_all_foreign_1M

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="save_foreign_asset_1M",
    default_args=default_args,
    start_date=datetime(2025,9,18,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 1 * * 1-5",
    catchup= False,
    tags=["DB", "foreign_asset"],
    max_active_runs=1
) as dag:

    foreign_asset_1M=PythonOperator(
        task_id='foreign_history_1M',
        python_callable=save_all_foreign_1M
    )
    

    foreign_asset_1M