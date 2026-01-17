from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.details.impact import impact

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}


with DAG(
    dag_id="impact",
    default_args=default_args,
    start_date=datetime(2025, 9, 11, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="* 9-15 * * 1-5",  
    catchup=False,
    tags=["DB", "market_data"]
) as dag:

    save_impact = PythonOperator(
        task_id='save_impact',
        python_callable=impact
    )
