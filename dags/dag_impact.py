from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.market_data.impact import impact

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
}

def wrapped_main(**context):
    now = datetime.now(tz=timezone("Asia/Ho_Chi_Minh"))
    # chỉ chạy 08:59 → 15:01
    if (now.hour == 8 and now.minute == 59) or \
       (9 <= now.hour < 15) or \
       (now.hour == 15 and now.minute <= 1):
        impact()
    else:
        print(f"Skip run at {now}")

with DAG(
    dag_id="impact",
    default_args=default_args,
    start_date=datetime(2025, 9, 11, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="* * * * *",   # chạy mỗi phút
    catchup=False,
    tags=["DB", "market_data"]
) as dag:

    save_impact = PythonOperator(
        task_id='save_impact',
        python_callable=wrapped_main
    )
