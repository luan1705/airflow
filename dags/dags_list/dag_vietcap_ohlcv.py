from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vietcap_ohlcv import save_DB_1D,save_DB_1
from utils.vietcap_ohlcv.List import generate_symbol_list

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="vietcap_ohlcv",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup= False,
    tags=["DB", "ETL"]
) as dag:
    update_symbol_list=PythonOperator(
        task_id='update_symbol_list',
        python_callable=generate_symbol_list
    )

    save_database_1D=PythonOperator(
        task_id='save_database_1D',
        python_callable=save_DB_1D
    )

    save_database_1=PythonOperator(
        task_id='save_database_1',
        python_callable=save_DB_1
    )

    update_symbol_list >> (save_database_1D,save_database_1)