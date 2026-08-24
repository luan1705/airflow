from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta, time as dtime
from pendulum import timezone, now
from utils.relative_strength.asset import etl_rs_rs_rank_today

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
    if cur.isoweekday() > 5:
        return False
    t = cur.time()
    return dtime(9, 0) <= t <= dtime(15, 0)

with DAG(
    dag_id="rs_symbol_today",
    default_args=default_args,
    start_date=datetime(2026, 5, 27, tzinfo=VN_TZ),
    schedule="0 9 * * 1-5",
    catchup=False,
    tags=["DB", "ETL"],
    max_active_runs=1,
) as dag:

    rs_rs_rank_symbol_today = PythonOperator(
        task_id="rs_rs_rank_symbol_today",
        python_callable=etl_rs_rs_rank_today,
    )

    gate_continue = ShortCircuitOperator(
        task_id="in_live_hours",
        python_callable=should_continue,
        trigger_rule="all_done"
    )

    wait = TimeDeltaSensor(
        task_id="wait_10s",
        delta=timedelta(seconds=10),
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="rs_symbol_today",
        wait_for_completion=False,
    )

    rs_rs_rank_symbol_today >> gate_continue >> wait >> trigger_next