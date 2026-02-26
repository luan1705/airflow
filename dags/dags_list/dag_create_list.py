from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.create_list import generate_symbol_list
from utils.create_list import generate_indice_map

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="create_list",
    default_args=default_args,
    start_date=datetime(2025,12,2,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="30 0 * * 1-5",
    catchup= False,
    tags=["DB", "ETL"]
) as dag:
    update_symbol_list=PythonOperator(
        task_id='update_symbol_list',
        python_callable=generate_symbol_list
    )

    update_indices_map=PythonOperator(
        task_id='update_indices_map',
        python_callable=generate_indice_map
    )

    

    (update_symbol_list , update_indices_map)