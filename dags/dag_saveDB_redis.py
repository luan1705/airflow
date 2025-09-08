from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
from utils.save import save_DB_1D,save_DB_1,save_rd

default_args = {
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    # 'retry_exponential_backoff': True,  # tùy chọn nếu muốn delay tăng dần
}

with DAG(
    dag_id="saveDatabase",
    default_args=default_args,
    start_date=datetime(2025,7,23,tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="15 15 * * 1-5",
    catchup= True,
    tags=["DB", "ETL"]
) as dag:

    save_database_1D=PythonOperator(
        task_id='save_database_1D',
        python_callable=save_DB_1D
    )

    save_database_1=PythonOperator(
        task_id='save_database_1',
        python_callable=save_DB_1
    )

    # save_redis=PythonOperator(
    #     task_id='save_redis',
    #     python_callable=save_rd
    # )

    (save_database_1D,save_database_1) # >> save_redis