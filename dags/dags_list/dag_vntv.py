from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values
from pytz import timezone
from sqlalchemy import create_engine

default_args = {
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
}


def sync_vntv_price():
    source_engine = create_engine(
        "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
    )
    target_engine = create_engine("postgresql+psycopg2://vntv:123456@aapanel.vntvcapital.com:5436/vntv")

    query = """
    SELECT symbol, close
    FROM details.asset
    ORDER BY symbol ASC
    """

    insert_sql = """
    INSERT INTO stock.stock ("symbol", "price")
    VALUES %s
    ON CONFLICT (symbol) DO UPDATE
    SET price = EXCLUDED.price
    """

    df = pd.read_sql(query, source_engine)
    df.rename(columns={"close": "price"}, inplace=True)

    values = [tuple(x) for x in df[["symbol", "price"]].to_numpy()]
    if not values:
        return

    conn = target_engine.raw_connection()
    cur = conn.cursor()
    try:
        execute_values(cur, insert_sql, values)
        conn.commit()
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id="vntv_price",
    default_args=default_args,
    start_date=datetime(2025, 12, 2, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule="0 9-15 * * 1-5",
    catchup=False,
    tags=["DB", "ETL"]
) as dag:

    sync_vntv_price_task = PythonOperator(
        task_id="sync_vntv_price",
        python_callable=sync_vntv_price,
    )

    sync_vntv_price_task
