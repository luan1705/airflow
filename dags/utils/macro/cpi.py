from datetime import datetime

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


def cpi():
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

    url = f"https://backend.miquant.vn/macro/cpi?from={from_date}&to={to_date}"

    print("Fetching CPI...")
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
        "cpi_all", "cpi_food", "cpi_food_serv", "cpi_crop",
        "cpi_alco", "cpi_cloth", "cpi_house", "cpi_housing",
        "cpi_health", "cpi_trans", "cpi_comm", "cpi_edu",
        "cpi_ent", "cpi_oth"
    ]

    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

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

        CREATE TABLE IF NOT EXISTS macro.cpi (
            time TIMESTAMP PRIMARY KEY,
            cpi_all DOUBLE PRECISION,
            cpi_food DOUBLE PRECISION,
            cpi_food_serv DOUBLE PRECISION,
            cpi_crop DOUBLE PRECISION,
            cpi_alco DOUBLE PRECISION,
            cpi_cloth DOUBLE PRECISION,
            cpi_house DOUBLE PRECISION,
            cpi_housing DOUBLE PRECISION,
            cpi_health DOUBLE PRECISION,
            cpi_trans DOUBLE PRECISION,
            cpi_comm DOUBLE PRECISION,
            cpi_edu DOUBLE PRECISION,
            cpi_ent DOUBLE PRECISION,
            cpi_oth DOUBLE PRECISION
        );
        """)

        # =========================
        # UPSERT (y chang bank rate)
        # =========================
        records = df[[
            "time",
            "cpi_all", "cpi_food", "cpi_food_serv", "cpi_crop",
            "cpi_alco", "cpi_cloth", "cpi_house", "cpi_housing",
            "cpi_health", "cpi_trans", "cpi_comm", "cpi_edu",
            "cpi_ent", "cpi_oth"
        ]].values.tolist()

        insert_sql = """
        INSERT INTO macro.cpi (
            time,
            cpi_all, cpi_food, cpi_food_serv, cpi_crop,
            cpi_alco, cpi_cloth, cpi_house, cpi_housing,
            cpi_health, cpi_trans, cpi_comm, cpi_edu,
            cpi_ent, cpi_oth
        ) VALUES %s
        ON CONFLICT (time) DO UPDATE SET
            cpi_all = EXCLUDED.cpi_all,
            cpi_food = EXCLUDED.cpi_food,
            cpi_food_serv = EXCLUDED.cpi_food_serv,
            cpi_crop = EXCLUDED.cpi_crop,
            cpi_alco = EXCLUDED.cpi_alco,
            cpi_cloth = EXCLUDED.cpi_cloth,
            cpi_house = EXCLUDED.cpi_house,
            cpi_housing = EXCLUDED.cpi_housing,
            cpi_health = EXCLUDED.cpi_health,
            cpi_trans = EXCLUDED.cpi_trans,
            cpi_comm = EXCLUDED.cpi_comm,
            cpi_edu = EXCLUDED.cpi_edu,
            cpi_ent = EXCLUDED.cpi_ent,
            cpi_oth = EXCLUDED.cpi_oth;
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

    print(f"DONE CPI {to_date}")