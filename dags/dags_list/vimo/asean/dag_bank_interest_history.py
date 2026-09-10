from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.asean import bank_interest_history_1m, bank_interest_history_3m, bank_interest_history_6m, bank_interest_history_1y, bank_interest_history_3y

default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="bank_interest_history",
    default_args=default_args,
    start_date=datetime(2026, 6, 4, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval= "0 6 * * 1-5",
    catchup=False,
    tags=["vimo", "bank_interest_history"]
) as dag:

    save_bank_interest_history_1m = PythonOperator(
        task_id="save_bank_interest_history_1m",
        python_callable=bank_interest_history_1m,
    )

    save_bank_interest_history_3m = PythonOperator(
        task_id="save_bank_interest_history_3m",
        python_callable=bank_interest_history_3m,
    )

    save_bank_interest_history_6m = PythonOperator(
        task_id="save_bank_interest_history_6m",
        python_callable=bank_interest_history_6m,
    )

    save_bank_interest_history_1y = PythonOperator(
        task_id="save_bank_interest_history_1y",
        python_callable=bank_interest_history_1y,
    )

    save_bank_interest_history_3y = PythonOperator(
        task_id="save_bank_interest_history_3y",
        python_callable=bank_interest_history_3y,
    )
    

