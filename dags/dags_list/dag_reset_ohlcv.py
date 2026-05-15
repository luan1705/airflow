from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from pendulum import timezone
import importlib

from utils.create_list import symbol_list
from utils.vietcap_ohlcv import save_DB_1D as reset_ohlcv


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def check_addition_not_empty():
    importlib.reload(symbol_list)

    if symbol_list.addition:
        print(f"addition có {len(symbol_list.addition)} mã: {symbol_list.addition}")
        return True

    print("addition rỗng, skip task chính")
    return False


with DAG(
    dag_id="reset_ohlcv",
    default_args=default_args,
    start_date=datetime(2026, 5, 5, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="30 8 * * *",
    catchup=False,
    tags=["DB", "reset_ohlcv"],
) as dag:

    check_addition = ShortCircuitOperator(
        task_id="check_addition",
        python_callable=check_addition_not_empty,
    )
    
    reset_ohlcv_task = PythonOperator(
        task_id="reset_ohlcv",
        python_callable=reset_ohlcv,
        do_xcom_push=False,
    )

    check_addition >> reset_ohlcv_task