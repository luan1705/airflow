from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.ohlcv_indices import save_DB_1D,save_DB_1
from utils.create_list.symbol_list import indices

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="ohlcv_indices",
    default_args=default_args,
    start_date=datetime(2026,8,19,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="15 15 * * 1-5",
    catchup= False,
    tags=["ohlcv", "indices", "vietcap"],
) as dag:

    save_database_1D=PythonOperator(
        task_id='save_database_1D',
        python_callable=save_DB_1D,
        op_kwargs={'symbol_list': indices}
    )

    save_database_1=PythonOperator(
        task_id='save_database_1',
        python_callable=save_DB_1,
        op_kwargs={'symbol_list': indices}
    )

    (save_database_1D,save_database_1)