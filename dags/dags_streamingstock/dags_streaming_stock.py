from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import time

default_args = {
    'owner': 'videv',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='stock_streaming',
    default_args=default_args,
    description='Run streaming_stock.py continuously from 9:00 to 15:00 Mon-Fri',
    schedule='0 9 * * 1-5',  # 9:00 AM Mon-Fri
    start_date=datetime(2025, 7, 28),
    catchup=False,
    tags=['stock', 'kafka', 'streaming'],
) as dag:

    def run_script(script_name):
        print(f"🚀 Starting {script_name} at {datetime.now()}")
        process = subprocess.Popen(['python3', f'/opt/airflow/dags/{script_name}'])

        try:
            while True:
                now = datetime.now()
                if now.hour >= 15:
                    print("🛑 Reached 15:00, stopping script...")
                    process.terminate()
                    break
                time.sleep(30)  # Check every 30 seconds
        except KeyboardInterrupt:
            process.terminate()

    streaming_task = PythonOperator(
        task_id='streaming_stock',
        python_callable=lambda: run_script('streaming_stock.py'),
    )
