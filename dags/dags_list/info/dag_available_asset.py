from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.info import available_asset
# from utils.available_asset.list import generate_symbol_list

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="available_asset",
    default_args=default_args,
    start_date=datetime(2025,12,2,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 9 * * 1-5",
    catchup= False,
    tags=["info", "asset", "available_asset"]
) as dag:

    available_asset_task=PythonOperator(
        task_id='available_asset',
        python_callable=available_asset
    )

    available_asset_task