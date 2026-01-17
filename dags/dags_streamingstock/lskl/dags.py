from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import time
import os

default_args = {
    'owner': 'videv',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def run_streaming():
    script_path = os.path.join(
        "/www/server/airflow/dags/dags_streamingstock/lskl",
        "streaming.py"
    )
    print(f"🚀 Starting {script_path} at {datetime.now()}")
    process = subprocess.Popen(['python3', script_path])

    try:
        while True:
            now = datetime.now()
            if now.hour >= 15:
                print(f"🛑 Reached {now.strftime('%H:%M')}, stopping script...")
                process.terminate()
                try:
                    process.wait(timeout=10)  # chờ process thoát
                except subprocess.TimeoutExpired:
                    print("⚠️ Script không thoát kịp, kill!")
                    process.kill()
                break
            time.sleep(30)  # check mỗi 30 giây
    except Exception as e:
        print(f"❌ Error: {e}")
        process.terminate()
        process.wait(timeout=10)

with DAG(
    dag_id='stock_streaming',
    default_args=default_args,
    description='Run streaming.py continuously from 9:00 to 15:00 Mon-Fri',
    schedule_interval='0 9 * * 1-5',  # chạy lúc 9:00 thứ 2-6
    start_date=datetime(2025, 9, 26),
    catchup=False,
    tags=['stock', 'kafka', 'streaming'],
    max_active_runs=1,
) as dag:

    streaming_task = PythonOperator(
        task_id='streaming_lskl',   # sửa lại không có khoảng trắng
        python_callable=run_streaming,
    )
