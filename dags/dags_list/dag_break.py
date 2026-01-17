from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.break_out_break_down import break_all
from utils.break_out_break_down.List import generate_symbol_list

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="break_dag",
    default_args=default_args,
    start_date=datetime(2025,12,2,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="15 15 * * 1-5",
    catchup= False,
    tags=["DB", "ETL"]
) as dag:
    update_symbol_list=PythonOperator(
        task_id='update_symbol_list',
        python_callable=generate_symbol_list
    )

    break_all_symbol=PythonOperator(
        task_id='tradingview_1D',
        python_callable=break_all
    )

    update_symbol_list >> break_all_symbol