from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta, time as dtime
from pendulum import timezone, now
import psycopg2

VN_TZ = timezone("Asia/Ho_Chi_Minh")

# Kết nối DB (đổi nếu cần)
CONN_STR = "postgresql://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=15),
}

# ====== Gate: chỉ cho chạy trước 15:00 (Mon–Fri) ======
def allow_run_in_time_window(**context) -> bool:
    cur = now(VN_TZ)

    # Thứ 2..6
    if cur.isoweekday() > 5:
        return False

    # Chỉ cho chạy trong 09:00–15:00
    t = cur.time()
    return dtime(9, 0) <= t <= dtime(15, 0)

# ====== Task thực thi TRUNCATE ======
def clear_eboard():
    # Chốt an toàn: dù bị trigger muộn hay trigger tay ngoài giờ cũng không xoá
    cur_time = now(VN_TZ).time()
    if cur_time > dtime(15, 0):
        return

    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE details.asset, details.dnse_asset;")
    cur.close()
    conn.close()

with DAG(
    dag_id="clear_eboard",
    default_args=default_args,
    start_date=datetime(2025, 9, 11, tzinfo=VN_TZ),
    schedule="1 9 * * 1-5",   # 09:01 thứ 2-6
    catchup=False,
    tags=["DB", "clear_eboard"],
) as dag:

    gate = ShortCircuitOperator(
        task_id="skip_if_outside_9_to_15",
        python_callable=allow_run_in_time_window,
    )

    run_clear = PythonOperator(
        task_id="clear_eboard",
        python_callable=clear_eboard,
    )

    gate >> run_clear
