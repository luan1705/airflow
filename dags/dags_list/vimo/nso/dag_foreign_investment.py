from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.foreign_investment import foreign_investment, foreign_investment_cal


with DAG(
    dag_id="vimo_foreign_investment",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "foreign_investment","word"],
) as dag:

    save_foreign_investment = PythonOperator(
        task_id="foreign_investment",
        python_callable=foreign_investment,
    )

    cal_foreign_investment = PythonOperator(
        task_id="foreign_investment_cal",
        python_callable=foreign_investment_cal,
    )

    save_foreign_investment >> cal_foreign_investment