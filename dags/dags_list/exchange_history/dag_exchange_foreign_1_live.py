from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, time as dtime
from pendulum import timezone, now
from airflow.sensors.time_delta import TimeDeltaSensor
from utils.exchange_history.foreign import main_1
from utils.details.foreign import foreign_HOSE, foreign_HNX, foreign_UPCOM 

VN_TZ = timezone("Asia/Ho_Chi_Minh")
default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

def should_continue() -> bool:
    """
    Chỉ cho phép tiếp tục (tức trigger vòng mới)
    trong khung 09:00–15:00, Thứ 2–Thứ 6 theo giờ VN.
    """
    cur = now(VN_TZ)
    if cur.isoweekday() > 5:  # 1..5 = Mon..Fri
        return False
    t = cur.time()
    return dtime(9, 0) <= t <= dtime(15, 0)

with DAG(
    dag_id="foreign_exchange_1_live",
    default_args=default_args,
    start_date=datetime(2025,12,16,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 9 * * 1-5",
    catchup= False,
    max_active_runs=1,
    tags=["DB", "market_data"]
) as dag:


    save_foreign_exchange_1 = PythonOperator(
        task_id='save_foreign_1',
        python_callable=main_1,
    )

    live_foreign_HNX = PythonOperator(
        task_id='live_foreign_HNX',
        python_callable=foreign_HNX,
    )

    live_foreign_HOSE = PythonOperator(
        task_id='live_foreign_HOSE',
        python_callable=foreign_HOSE,
    )

    live_foreign_UPCOM = PythonOperator(
        task_id='live_foreign_UPCOM',
        python_callable=foreign_UPCOM,
    )

    gate_continue = ShortCircuitOperator(
        task_id="in_live_hours",
        python_callable=should_continue,
    )

    wait_2s = TimeDeltaSensor(
        task_id="wait_2s",
        delta=timedelta(seconds=2),
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="foreign_exchange_1_live",
        wait_for_completion=False,
    )
    
    save_foreign_exchange_1 >> [
    live_foreign_HNX,
    live_foreign_HOSE,
    live_foreign_UPCOM
] >> gate_continue >> wait_2s >> trigger_next