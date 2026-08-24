from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from pendulum import timezone
import importlib

from utils.create_list import symbol_list


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def check_list_not_empty():
    importlib.reload(symbol_list)
    
    adjust_list = list(dict.fromkeys(symbol_list.price_diff + symbol_list.dividend_date))
    
    if adjust_list:
        print(f"adjust_list có {len(adjust_list)} mã: {adjust_list}")
        return True

    print("adjust_list rỗng, skip")
    return False

def run_reset_ohlcv():
    importlib.reload(symbol_list)
    from utils.vietcap_ohlcv import save_DB_1D
    adjust_list = list(dict.fromkeys(symbol_list.price_diff + symbol_list.dividend_date))
    print(f"adjust_list có {len(adjust_list)} mã: {adjust_list}")
    return save_DB_1D(symbol_list=adjust_list)


with DAG(
    dag_id="reset_ohlcv",
    default_args=default_args,
    start_date=datetime(2026, 5, 5, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="16 9 * * *",
    catchup=False,
    tags=["ohlcv_check", "reset_ohlcv"],
) as dag:

    check_addition = ShortCircuitOperator(
        task_id="check_list_not_empty",
        python_callable=check_list_not_empty,
    )
    
    reset_ohlcv_task = PythonOperator(
        task_id="reset_ohlcv",
        python_callable=run_reset_ohlcv,
        do_xcom_push=False,
    )

    check_addition >> reset_ohlcv_task