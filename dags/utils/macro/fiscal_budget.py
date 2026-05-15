from datetime import datetime

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


def fiscal_budget():
    # =========================
    # TIME RANGE (month hiện tại + tháng trước)
    # =========================
    now = datetime.now()

    first_day_this_month = now.replace(day=1)
    last_month = first_day_this_month - pd.DateOffset(months=1)
    first_day_last_month = last_month.replace(day=1)

    from_date = first_day_last_month.strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    # from_date="2005-11-18"
    # to_date="2025-11-18"

    # =========================
    # API
    # =========================
    headers = {
        'accept': 'application/json, text/plain, */*',
        'origin': 'https://app.miquant.vn',
        'referer': 'https://app.miquant.vn/',
        'user-agent': 'Mozilla/5.0'
    }

    url = f"https://backend.miquant.vn/macro/macro-fiscal-budget?from={from_date}&to={to_date}"

    print("Fetching fiscal_budget...")
    res = requests.get(url, headers=headers, timeout=30)
    res.raise_for_status()

    data = res.json()
    df = pd.DataFrame(data["data"])

    if df.empty:
        print("No data returned")
        return

    # =========================
    # TIME normalize
    # =========================
    df["time"] = (
        pd.to_datetime(df["timestamp"], unit="s", utc=True)
        .dt.tz_convert("Asia/Ho_Chi_Minh")
        .dt.normalize()
    )

    # =========================
    # CLEAN DATA
    # =========================
    df["budget_deficit_bil_vnd"] = pd.to_numeric(df["budget_deficit_bil_vnd"], errors="coerce")

    df = df.astype(object).where(pd.notnull(df), None)

    # =========================
    # DB CONNECT
    # =========================
    conn = psycopg2.connect(
        host="videv.cloud",
        database="vnsfintech",
        user="vnsfintech",
        password="Vns_123456",
        port=5433
    )
    cursor = conn.cursor()

    try:
        # =========================
        # CREATE TABLE
        # =========================
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS macro;

        CREATE TABLE IF NOT EXISTS macro.fiscal_budget (
            time TIMESTAMP PRIMARY KEY,
            budget_deficit_bil_vnd DOUBLE PRECISION
        );
        """)

        # =========================
        # UPSERT
        # =========================
        records = df[[
            "time",
            "budget_deficit_bil_vnd"
        ]].values.tolist()

        insert_sql = """
        INSERT INTO macro.fiscal_budget (
            time,
            budget_deficit_bil_vnd
        ) VALUES %s
        ON CONFLICT (time) DO UPDATE SET
            budget_deficit_bil_vnd = EXCLUDED.budget_deficit_bil_vnd;
        """

        execute_values(cursor, insert_sql, records)

        conn.commit()
        print(f"Inserted/Updated {len(records)} rows")

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()

    print(f"DONE fiscal_budget {to_date}")