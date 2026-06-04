from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.relative_strength.sector import rs_rank_sector

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="rs_rank_sector",
    default_args=default_args,
    start_date=datetime(2026, 5, 25, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule=None,
    catchup=False,
    tags=["DB", "ETL"]
) as dag:

    hose = PythonOperator(
        task_id='rs_rank_sector_HOSE',
        python_callable=rs_rank_sector,
        op_kwargs={'exchange': 'HOSE', 'benchmark': 'VNINDEX_1D'}
    )

    hnx = PythonOperator(
        task_id='rs_rank_sector_HNX',
        python_callable=rs_rank_sector,
        op_kwargs={'exchange': 'HNX', 'benchmark': 'HNXINDEX_1D'}
    )

    upcom = PythonOperator(
        task_id='rs_rank_sector_UPCOM',
        python_callable=rs_rank_sector,
        op_kwargs={'exchange': 'UPCOM', 'benchmark': 'UPCOMINDEX_1D'}
    )
