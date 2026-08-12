from datetime import datetime, timedelta, time as dtime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from pendulum import now, timezone

from utils.exchange_history.breadth_distribution import breadth_distribution


VN_TZ = timezone("Asia/Ho_Chi_Minh")

default_args = {
    "retries": 50,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def should_continue() -> bool:
    current = now(VN_TZ)

    if current.isoweekday() > 5:
        return False

    return dtime(9, 0) <= current.time() < dtime(15, 0)


with DAG(
    dag_id="breadth_distribution",
    default_args=default_args,
    start_date=datetime(2025, 9, 18, tzinfo=VN_TZ),
    schedule="0 9 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["exchange_history", "breadth_distribution"],
) as dag:

    save_breadth_distribution = PythonOperator(
        task_id="save_breadth_distribution",
        python_callable=breadth_distribution,
    )

    gate_continue = ShortCircuitOperator(
        task_id="in_live_hours",
        python_callable=should_continue,
    )

    wait = TimeDeltaSensor(
        task_id="wait_2s",
        delta=timedelta(seconds=2),
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="breadth_distribution",
        wait_for_completion=False,
    )

    save_breadth_distribution >> gate_continue >> wait >> trigger_next