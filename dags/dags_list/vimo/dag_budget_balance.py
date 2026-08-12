from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.budget_balance import budget_balance, budget_balance_cal


with DAG(
    dag_id="vimo_budget_balance",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "budget_balance","word"],
) as dag:

    save_budget_balance = PythonOperator(
        task_id="budget_balance",
        python_callable=budget_balance,
    )

    cal_budget_balance = PythonOperator(
        task_id="budget_balance_cal",
        python_callable=budget_balance_cal,
    )

    save_budget_balance >> cal_budget_balance
