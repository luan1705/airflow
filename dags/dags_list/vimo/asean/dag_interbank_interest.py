from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.asean import interbank_interest_on, interbank_interest_1w, interbank_interest_2w, interbank_interest_1m, interbank_interest_3m, interbank_interest_6m

default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="interbank_interest",
    default_args=default_args,
    start_date=datetime(2026, 6, 4, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval= "0 6 * * 1-5",
    catchup=False,
    tags=["vimo", "interbank_interest"]
) as dag:

    save_interbank_interest_on = PythonOperator(
        task_id="save_interbank_interest_on",
        python_callable=interbank_interest_on,
    )

    save_interbank_interest_1w = PythonOperator(
        task_id="save_interbank_interest_1w",
        python_callable=interbank_interest_1w,
    )

    save_interbank_interest_2w = PythonOperator(
        task_id="save_interbank_interest_2w",
        python_callable=interbank_interest_2w,
    )

    save_interbank_interest_1m = PythonOperator(
        task_id="save_interbank_interest_1m",
        python_callable=interbank_interest_1m,
    )

    save_interbank_interest_3m = PythonOperator(
        task_id="save_interbank_interest_3m",
        python_callable=interbank_interest_3m,
    )

    save_interbank_interest_6m = PythonOperator(
        task_id="save_interbank_interest_6m",
        python_callable=interbank_interest_6m,
    )

    save_interbank_interest_on >> [save_interbank_interest_1w, save_interbank_interest_2w , save_interbank_interest_1m , save_interbank_interest_3m , save_interbank_interest_6m]