from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.exchange_indices import upsert_ex_in
from utils.exchange_indices.List import generate_exchange_list, generate_indice_map

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="upsert_ex_in",
    default_args=default_args,
    start_date=datetime(2025,12,15,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="15 20 * * 1-5",
    catchup= False,
    tags=["DB", "ETL"]
) as dag:
    update_exchange_list=PythonOperator(
        task_id='update_exchange_list',
        python_callable=generate_exchange_list
    )
    update_indice_map=PythonOperator(
        task_id='update_indice_map',
        python_callable=generate_indice_map
    )

    get_ex_in=PythonOperator(
        task_id='get_ex_in',
        python_callable=upsert_ex_in
    )

    [update_exchange_list >> update_indice_map] >> get_ex_in