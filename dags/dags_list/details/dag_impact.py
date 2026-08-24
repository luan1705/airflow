from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta, time as dtime
from pendulum import timezone, now

from utils.details.impact import impact


VN_TZ = timezone("Asia/Ho_Chi_Minh")


default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def should_continue() -> bool:
    cur = now(VN_TZ)

    # Thứ 7, CN thì dừng
    if cur.isoweekday() > 5:
        return False

    t = cur.time()

    # Chỉ tiếp tục self-trigger trong 09:00 - 15:00
    return dtime(9, 0) <= t <= dtime(15, 0)


with DAG(
    dag_id="details_impact",
    default_args=default_args,
    start_date=datetime(2025, 9, 11, tzinfo=VN_TZ),

    # Chỉ cron kích hoạt lần đầu lúc 09:00 T2-T6
    schedule="0 9 * * 1-5",

    catchup=False,
    tags=["details", "impact", "asset"],

    # Không cho nhiều run của DAG chạy song song
    max_active_runs=1,
) as dag:

    save_impact = PythonOperator(
        task_id="save_impact",
        python_callable=impact,
    )

    gate_continue = ShortCircuitOperator(
        task_id="in_live_hours",
        python_callable=should_continue,

        # impact fail thì vẫn chạy gate sau khi retry hết
        trigger_rule="all_done",
    )

    wait = TimeDeltaSensor(
        task_id="wait_3s",
        delta=timedelta(seconds=3),
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="details_impact",
        wait_for_completion=False,
    )

    save_impact >> gate_continue >> wait >> trigger_next