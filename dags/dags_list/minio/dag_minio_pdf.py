from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.minio import (
    fetch_news,
    find_pdf,
    upload_minio,
    publish_pdf,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pdf_minio_pipeline",
    description="Vietcap -> Find PDF -> MinIO -> Publish PDF metadata",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="0 6 * * 2-5",   # 06:00 mỗi ngày
    catchup=False,
    max_active_runs=1,
    tags=["pdf", "document", "minio"],
) as dag:

    task_fetch_news = PythonOperator(
        task_id="fetch_news",
        python_callable=fetch_news,
    )

    task_find_pdf = PythonOperator(
        task_id="find_pdf",
        python_callable=find_pdf,
    )

    task_upload_minio = PythonOperator(
        task_id="upload_minio",
        python_callable=upload_minio,
    )

    task_publish_pdf = PythonOperator(
        task_id="publish_pdf",
        python_callable=publish_pdf,
    )

    task_fetch_news >> task_find_pdf >> task_upload_minio >> task_publish_pdf