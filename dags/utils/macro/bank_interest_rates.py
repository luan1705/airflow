from datetime import datetime

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


def bank_interest_rates():
    # =========================
    # TIME RANGE (year now + prev)
    # =========================
    # # YEARS
    # now = datetime.now()
    # year_now = now.year
    # year_prev = year_now - 1

    # from_date = f"{year_prev}-01-01"
    # to_date = now.strftime("%Y-%m-%d")
    #==========================
    # MONTHS
    now = datetime.now()

    # đầu tháng hiện tại
    first_day_this_month = now.replace(day=1)

    # đầu tháng trước
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



    url = f"https://backend.miquant.vn/macro/bank-interest?from={from_date}&to={to_date}"

    print("Fetching data...")
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

        CREATE TABLE IF NOT EXISTS macro.bank_interest_rates (
            time TIMESTAMP PRIMARY KEY,
            term_on  DOUBLE PRECISION,
            term_1w  DOUBLE PRECISION,
            term_2w  DOUBLE PRECISION,
            term_1m  DOUBLE PRECISION,
            term_2m  DOUBLE PRECISION,
            term_3m  DOUBLE PRECISION,
            term_6m  DOUBLE PRECISION,
            term_9m  DOUBLE PRECISION,
            term_1y  DOUBLE PRECISION
        );
        """)

        # =========================
        # UPSERT (khuyến nghị hơn truncate)
        # =========================
        records = df[[
            "time",
            "term_on", "term_1w", "term_2w",
            "term_1m", "term_2m", "term_3m",
            "term_6m", "term_9m", "term_1y"
        ]].values.tolist()

        insert_sql = """
        INSERT INTO macro.bank_interest_rates (
            time,
            term_on, term_1w, term_2w,
            term_1m, term_2m, term_3m,
            term_6m, term_9m, term_1y
        ) VALUES %s
        ON CONFLICT (time) DO UPDATE SET
            term_on = EXCLUDED.term_on,
            term_1w = EXCLUDED.term_1w,
            term_2w = EXCLUDED.term_2w,
            term_1m = EXCLUDED.term_1m,
            term_2m = EXCLUDED.term_2m,
            term_3m = EXCLUDED.term_3m,
            term_6m = EXCLUDED.term_6m,
            term_9m = EXCLUDED.term_9m,
            term_1y = EXCLUDED.term_1y;
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

    print(f"DONE bank_interest_rates {to_date}")