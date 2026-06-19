from datetime import datetime

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


def credit_money_supply():
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

    url = f"https://backend.miquant.vn/macro/credit-money-supply?from={from_date}&to={to_date}"

    print("Fetching credit_money_supply...")
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
    df["creditgrowth_ytd"] = pd.to_numeric(df["creditgrowth_ytd"], errors="coerce")
    df["moneysupplym2_ytd"] = pd.to_numeric(df["moneysupplym2_ytd"], errors="coerce")

    df = df.astype(object).where(pd.notnull(df), None)

    # =========================
    # DB CONNECT
    # =========================
    conn = psycopg2.connect(
        host="tanhungsoft.com",
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

        CREATE TABLE IF NOT EXISTS macro.credit_money_supply (
            time TIMESTAMP PRIMARY KEY,
            creditgrowth_ytd DOUBLE PRECISION,
            moneysupplym2_ytd DOUBLE PRECISION
        );
        """)

        # =========================
        # UPSERT (giống bank rate)
        # =========================
        records = df[[
            "time",
            "creditgrowth_ytd",
            "moneysupplym2_ytd"
        ]].values.tolist()

        insert_sql = """
        INSERT INTO macro.credit_money_supply (
            time,
            creditgrowth_ytd,
            moneysupplym2_ytd
        ) VALUES %s
        ON CONFLICT (time) DO UPDATE SET
            creditgrowth_ytd = EXCLUDED.creditgrowth_ytd,
            moneysupplym2_ytd = EXCLUDED.moneysupplym2_ytd;
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

    print(f"DONE credit_money_supply {to_date}")