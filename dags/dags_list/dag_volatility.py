from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta, time as dtime
from pendulum import timezone, now

from utils.details.volatility import volatility_HOSE, volatility_HNX, volatility_UPCOM

VN_TZ = timezone("Asia/Ho_Chi_Minh")

default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=2),
    "depends_on_past": False,
}

def should_continue() -> bool:
    """
    Chỉ cho phép trigger vòng mới trong khung 09:00–15:00, Thứ 2–Thứ 6 (giờ VN).
    """
    cur = now(VN_TZ)
    if cur.isoweekday() > 5:  # 1..5 = Mon..Fri
        return False
    t = cur.time()
    return dtime(9, 0) <= t < dtime(15, 0)  # 15:00 thì dừng (không trigger thêm)

with DAG(
    dag_id="exchange_volatility",
    default_args=default_args,
    start_date=datetime(2025, 12, 16, tzinfo=VN_TZ),
    schedule="0 9 * * 1-5",   # khởi động 09:00 Thứ 2–Thứ 6
    catchup=False,
    tags=["DB", "market_data"],
    max_active_runs=1,        # tránh chồng run
) as dag:

    t_hose = PythonOperator(task_id="volatility_HOSE", python_callable=volatility_HOSE)
    t_hnx  = PythonOperator(task_id="volatility_HNX",  python_callable=volatility_HNX)
    t_upc  = PythonOperator(task_id="volatility_UPCOM",python_callable=volatility_UPCOM)

    gate_continue = ShortCircuitOperator(
        task_id="in_live_hours",
        python_callable=should_continue,
    )

    # ✅ Delay để tránh trigger liên tục => giảm spam DB/connection
    wait_2s = TimeDeltaSensor(
        task_id="wait_2s",
        delta=timedelta(seconds=2),
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="exchange_volatility",
        wait_for_completion=False,
    )

    [t_hose, t_hnx, t_upc] >> gate_continue >> wait_2s >> trigger_next
