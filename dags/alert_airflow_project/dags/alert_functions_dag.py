from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    "owner": "alert_system",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

BASE_ALERT_PATH = "/www/server/airflow/dags/alert_airflow_project/alert_scripts"

def run_alert_script(script_name):
    now = datetime.now()
    if not (now.weekday() < 5 and 9 <= now.hour < 15):
        print(f"⏹ Outside trading time: {now}. Skipping {script_name}...")
        return

    script_path = os.path.join(BASE_ALERT_PATH, script_name)
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"❌ Script not found: {script_path}")

    try:
        print(f"[INFO] Running script: {script_path}")
        result = subprocess.run(
            ["python3", script_path],
            text=True,
            capture_output=True,
            check=True
        )
        print(result.stdout)
        if result.stderr:
            print("[WARN]", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {script_name} failed:\n{e.stderr}")
        raise

alert_scripts = [
    "Alert_Intraday.py",
    "Alert_MA.py",
    "Alert_MACD.py",
    "Alert_MAcross.py",
    "Alert_Setup.py",
    "Alert_Volume.py"
]

with DAG(
    dag_id="run_all_alerts_and_save_to_postgres",
    default_args=default_args,
    description="Run all alert scripts in parallel and save to Postgres",
    schedule_interval="*/5 9-15 * * 1-5",
    start_date=datetime(2025, 8, 18),
    catchup=False,
    tags=["alerts", "postgres"]
) as dag:

    for script in alert_scripts:
        PythonOperator(
            task_id=f"run_{script.split('.')[0].lower()}",
            python_callable=run_alert_script,
            op_args=[script]
        )
