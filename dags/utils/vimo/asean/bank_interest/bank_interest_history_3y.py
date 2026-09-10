import logging
from datetime import datetime

import pytz
import requests
import pandas as pd

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from db_config import POSTGRES_URL


log = logging.getLogger(__name__)


def bank_interest_history_3y():

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://research.aseansc.com.vn/",
        "Origin": "https://research.aseansc.com.vn",
    }

    url = "https://asean-apigw.aseansc.com.vn/pbapi/api/macro/interestBankHist"

    params = {
        "interestPeriod": "36M"
    }

    tz = pytz.timezone("Asia/Ho_Chi_Minh")

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=30
    )

    response.raise_for_status()

    json_data = response.json()

    if "data" not in json_data:
        raise ValueError(
            f"API không có key 'data': {json_data}"
        )

    if not json_data["data"]:
        log.info("API bank_interest_3y không có dữ liệu mới.")
        return 0

    df = pd.DataFrame(json_data["data"])

    required_columns = [
        "report_Date",
        "SOB",
        "LARGE",
        "SMALL",
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"API bank_interest_3y thiếu columns: {missing_columns}"
        )

    df = df[required_columns].copy()

    df["time"] = pd.to_datetime(
        df["report_Date"],
        format="%d/%m/%Y",
        errors="coerce"
    ).dt.date

    df = df.rename(columns={
        "SOB": "sob",
        "LARGE": "large",
        "SMALL": "small"
    })

    for col in ["sob", "large", "small"]:
        df[col] = (
            pd.to_numeric(df[col], errors="coerce") / 100
        ).round(5)

    df = df[
        ["time", "sob", "large", "small"]
    ].copy()

    df = (
        df
        .dropna(subset=["time"])
        .drop_duplicates(subset=["time"], keep="last")
        .sort_values("time")
        .reset_index(drop=True)
    )

    engine = create_engine(
        POSTGRES_URL
    )

    with engine.begin() as con:
        con.execute(text("""
            CREATE SCHEMA IF NOT EXISTS macro
        """))

        con.execute(text("""
            CREATE TABLE IF NOT EXISTS macro.bank_interest_3y (
                "time" date PRIMARY KEY,
                "sob" double precision,
                "large" double precision,
                "small" double precision
            )
        """))

    rows = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in df.itertuples(index=False, name=None)
    ]

    if not rows:
        log.info("Không có dữ liệu bank_interest_3y để ghi DB.")
        return 0

    sql = """
        INSERT INTO macro.bank_interest_3y
        ("time", "sob", "large", "small")
        VALUES %s

        ON CONFLICT ("time")
        DO UPDATE SET
            "sob" = EXCLUDED."sob",
            "large" = EXCLUDED."large",
            "small" = EXCLUDED."small"
    """

    conn = engine.raw_connection()

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)

        conn.commit()

        now = datetime.now(tz)

        log.info(
            f"✅ Đã upsert {len(rows)} dòng vào "
            f"macro.bank_interest_3y | "
            f"{df['time'].min()} → {df['time'].max()} "
            f"lúc {now.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    return len(rows)


if __name__ == "__main__":
    bank_interest_history_3y()