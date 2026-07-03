from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta, time as dtime
from pendulum import timezone, now
from utils.olchv_indices import save_olch

VN_TZ = timezone("Asia/Ho_Chi_Minh")

default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=10)
}

def should_continue() -> bool:
    cur = now(VN_TZ)
    if cur.isoweekday() > 5:
        return False
    t = cur.time()
    return dtime(9, 0) <= t <= dtime(15, 0)

with DAG(
    dag_id="upsert_olchv_indices",
    default_args=default_args,
    start_date=datetime(2025,9,11,tzinfo=VN_TZ),
    schedule="0 9 * * 1-5",
    catchup=False,
    tags=["dashboard", "indices", "olchv"],
    max_active_runs=1,
) as dag:

    upsert_olchv_indices = PythonOperator(
        task_id="upsert_olchv_indices",
        python_callable=save_olch,
    )

    wait = TimeDeltaSensor(
        task_id="wait_1s",
        delta=timedelta(seconds=1),
        mode='reschedule',
    )

    gate_continue = ShortCircuitOperator(
        task_id="in_live_hours",
        python_callable=should_continue,
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="upsert_olchv_indices",
        wait_for_completion=False,
    )

    upsert_olchv_indices >> gate_continue >> wait >> trigger_next