from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.active_asset import active_asset
# from utils.active_asset.list import generate_symbol_list

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="active_asset",
    default_args=default_args,
    start_date=datetime(2025,12,2,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 9 * * 1-5",
    catchup= False,
    tags=["DB", "ETL"]
) as dag:

    active_asset_task=PythonOperator(
        task_id='active_asset',
        python_callable=active_asset
    )

    active_asset_task