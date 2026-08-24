from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta, time as dtime
from pendulum import timezone, now
from utils.exchange_history.breadth.today import pepb_breadth_today

VN_TZ = timezone("Asia/Ho_Chi_Minh")

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=2),
    "depends_on_past": False,
}

def should_continue() -> bool:
    cur = now(VN_TZ)
    if cur.isoweekday() > 5:
        return False
    t = cur.time()
    return dtime(9, 0) <= t <= dtime(15, 0)

with DAG(
    dag_id="breadth_pepb_today",
    default_args=default_args,
    start_date=datetime(2026, 5, 27, tzinfo=VN_TZ),
    schedule="0 9 * * 1-5",
    catchup=False,
    tags=["exchange_history", "breadth", "adv_dec"],
    max_active_runs=1,
) as dag:

    pepb_today_task = PythonOperator(
        task_id="pepb_today",
        python_callable=pepb_breadth_today,
    )

    gate_continue = ShortCircuitOperator(
        task_id="in_live_hours",
        python_callable=should_continue,
        trigger_rule="all_done"
    )

    wait = TimeDeltaSensor(
        task_id="wait_10s",
        delta=timedelta(seconds=10),
        mode='reschedule',
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="breadth_pepb_today",
        wait_for_completion=False,
    )

    pepb_today_task >> gate_continue >> wait >> trigger_next