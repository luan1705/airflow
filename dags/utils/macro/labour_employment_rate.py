from datetime import datetime

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


def labour_employment_rate():
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

    url = f"https://backend.miquant.vn/macro/labour-employment-rate?from={from_date}&to={to_date}"

    print("Fetching labour_employment_rate...")
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
    df["unemployment_rate"] = pd.to_numeric(df["unemployment_rate"], errors="coerce")
    df["labour_force_thousand"] = pd.to_numeric(df["labour_force_thousand"], errors="coerce")

    df = df.astype(object).where(pd.notnull(df), None)

    # =========================
    # DB CONNECT
    # =========================
    conn = psycopg2.connect("postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl")
    cursor = conn.cursor()

    try:
        # =========================
        # CREATE TABLE
        # =========================
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS macro;

        CREATE TABLE IF NOT EXISTS macro.labour_employment_rate (
            time TIMESTAMP PRIMARY KEY,
            unemployment_rate DOUBLE PRECISION,
            labour_force_thousand DOUBLE PRECISION
        );
        """)

        # =========================
        # UPSERT
        # =========================
        records = df[[
            "time",
            "unemployment_rate",
            "labour_force_thousand"
        ]].values.tolist()

        insert_sql = """
        INSERT INTO macro.labour_employment_rate (
            time,
            unemployment_rate,
            labour_force_thousand
        ) VALUES %s
        ON CONFLICT (time) DO UPDATE SET
            unemployment_rate = EXCLUDED.unemployment_rate,
            labour_force_thousand = EXCLUDED.labour_force_thousand;
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

    print(f"DONE labour_employment_rate {to_date}")