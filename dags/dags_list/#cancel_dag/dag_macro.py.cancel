from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone

from utils.macro import (
    bank_interest_rates,
    cpi,
    credit_money_supply,
    export_import,
    fdi,
    fiscal_budget,
    gdp_growth,
    index_industrial_production,
    international_visitors,
    labour_employment_rate,
    public_invest,
    retail_sales,
)

default_args = {
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="macro",
    default_args=default_args,
    start_date=datetime(2026, 5, 4, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="0 0 5 * *",   # ngày 5 hàng tháng
    catchup=False,
    tags=["DB", "macro"]
) as dag:

    save_bank_interest = PythonOperator(
        task_id='bank_interest',
        python_callable=bank_interest_rates,
    )

    save_cpi = PythonOperator(
        task_id='cpi',
        python_callable=cpi,
    )

    save_credit = PythonOperator(
        task_id='credit_money_supply',
        python_callable=credit_money_supply,
    )

    save_trade = PythonOperator(
        task_id='export_import',
        python_callable=export_import,
    )

    save_fdi = PythonOperator(
        task_id='fdi',
        python_callable=fdi,
    )

    save_fiscal = PythonOperator(
        task_id='fiscal_budget',
        python_callable=fiscal_budget,
    )

    save_gdp = PythonOperator(
        task_id='gdp_growth',
        python_callable=gdp_growth,
    )

    save_iip = PythonOperator(
        task_id='iip',
        python_callable=index_industrial_production,
    )

    save_tourism = PythonOperator(
        task_id='international_visitors',
        python_callable=international_visitors,
    )

    save_labour = PythonOperator(
        task_id='labour',
        python_callable=labour_employment_rate,
    )

    save_public = PythonOperator(
        task_id='public_invest',
        python_callable=public_invest,
    )

    save_retail = PythonOperator(
        task_id='retail_sales',
        python_callable=retail_sales,
    )

    # =========================
    # RUN SONG SONG
    # =========================
    [
        save_bank_interest,
        save_cpi,
        save_credit,
        save_trade,
        save_fdi,
        save_fiscal,
        save_gdp,
        save_iip,
        save_tourism,
        save_labour,
        save_public,
        save_retail,
    ]