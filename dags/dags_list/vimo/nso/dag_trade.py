from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.vimo.source_code.trade import trade, trade_cal


with DAG(
    dag_id="vimo_trade",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    start_date=datetime(2026, 5, 27, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False, 
    tags=["vimo", "trade","excel"],
) as dag:

    save_trade = PythonOperator(
        task_id="trade",
        python_callable=trade,
    )

    cal_trade = PythonOperator(
        task_id="trade_cal",
        python_callable=trade_cal,
    )

    save_trade >> cal_trade