from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.budget_expenditure import budget_expenditure, budget_expenditure_cal


with DAG(
    dag_id="vimo_budget_expenditure",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "budget_expenditure","word"],
) as dag:

    save_budget_expenditure = PythonOperator(
        task_id="budget_expenditure",
        python_callable=budget_expenditure,
    )

    cal_budget_expenditure = PythonOperator(
        task_id="budget_expenditure_cal",
        python_callable=budget_expenditure_cal,
    )

    save_budget_expenditure >> cal_budget_expenditure
