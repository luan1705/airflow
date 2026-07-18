from datetime import datetime

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


def index_industrial_production():
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

    url = f"https://backend.miquant.vn/macro/index-industrial-production?from={from_date}&to={to_date}"

    print("Fetching index_industrial_production...")
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
    cols = [
        "yoy_iip_all",
        "yoy_iip_ep",
        "yoy_iip_mip",
        "yoy_iip_mnp",
        "yoy_iip_wm"
    ]

    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

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

        CREATE TABLE IF NOT EXISTS macro.index_industrial_production (
            time TIMESTAMP PRIMARY KEY,
            yoy_iip_all DOUBLE PRECISION,
            yoy_iip_ep DOUBLE PRECISION,
            yoy_iip_mip DOUBLE PRECISION,
            yoy_iip_mnp DOUBLE PRECISION,
            yoy_iip_wm DOUBLE PRECISION
        );
        """)

        # =========================
        # UPSERT
        # =========================
        records = df[[
            "time",
            "yoy_iip_all",
            "yoy_iip_ep",
            "yoy_iip_mip",
            "yoy_iip_mnp",
            "yoy_iip_wm"
        ]].values.tolist()

        insert_sql = """
        INSERT INTO macro.index_industrial_production (
            time,
            yoy_iip_all,
            yoy_iip_ep,
            yoy_iip_mip,
            yoy_iip_mnp,
            yoy_iip_wm
        ) VALUES %s
        ON CONFLICT (time) DO UPDATE SET
            yoy_iip_all = EXCLUDED.yoy_iip_all,
            yoy_iip_ep = EXCLUDED.yoy_iip_ep,
            yoy_iip_mip = EXCLUDED.yoy_iip_mip,
            yoy_iip_mnp = EXCLUDED.yoy_iip_mnp,
            yoy_iip_wm = EXCLUDED.yoy_iip_wm;
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

    print(f"DONE index_industrial_production {to_date}")