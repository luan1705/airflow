from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, time as dtime
from pendulum import timezone, now
from utils.asset_history_symbol import save_all_foreign_today

VN_TZ = timezone("Asia/Ho_Chi_Minh")

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
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
    dag_id="save_foreign_asset_today",
    default_args=default_args,
    start_date=datetime(2025, 9, 18, tzinfo=VN_TZ),
    schedule="0 9 * * 1-5",  # Tự khởi động 09:00 T2–T6
    catchup=False,
    tags=["DB", "foreign_asset"],
    max_active_runs=1,
) as dag:

    # Task chính
    foreign_asset_today = PythonOperator(
        task_id="foreign_history_today",
        python_callable=save_all_foreign_today,
    )

    # Kiểm tra khung thời gian hợp lệ
    gate_continue = ShortCircuitOperator(
        task_id="in_live_hours",
        python_callable=should_continue,
    )

    # Tự trigger lại chính DAG nếu trong khung giờ
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="save_foreign_asset_today",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    foreign_asset_today >> gate_continue >> trigger_next
