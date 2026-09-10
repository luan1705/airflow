import logging
from datetime import datetime

import pytz
import requests
import pandas as pd

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from db_config import POSTGRES_URL


log = logging.getLogger(__name__)


def bank_interest():

    # ========================================================
    # CONFIG
    # ========================================================

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://research.aseansc.com.vn/",
        "Origin": "https://research.aseansc.com.vn",
    }

    url = "https://asean-apigw.aseansc.com.vn/pbapi/api/macro/interestbank"

    tz = pytz.timezone("Asia/Ho_Chi_Minh")

    # ========================================================
    # CALL API
    # ========================================================

    response = requests.get(
        url,
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
        log.info("API bank_interest không có dữ liệu mới.")
        return 0

    # ========================================================
    # DATAFRAME
    # ========================================================

    df = pd.DataFrame(
        json_data["data"]
    )

    required_columns = [
        "tradeDate",
        "bankCode",
        "bankName",
        "bankNameEn",
        "interest1M",
        "interest3M",
        "interest6M",
        "interest13M",
        "interest36M",
    ]

    missing_columns = [
        col
        for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"API bank_interest thiếu columns: {missing_columns}"
        )

    df = df[
        required_columns
    ].copy()

    # ========================================================
    # DATE
    # ========================================================

    df["tradeDate"] = pd.to_datetime(
        df["tradeDate"],
        errors="coerce"
    ).dt.date

    # ========================================================
    # RENAME
    # ========================================================

    df = df.rename(
        columns={
            "tradeDate": "time",
            "bankCode": "symbol",
            "bankName": "bankNameVi",
            "bankNameEn": "bankNameEn",
            "interest1M": "interest1m",
            "interest3M": "interest3m",
            "interest6M": "interest6m",
            "interest13M": "interest1y",
            "interest36M": "interest3y",
        }
    )

    # ========================================================
    # PERCENT -> DECIMAL
    # ========================================================

    interest_columns = [
        "interest1m",
        "interest3m",
        "interest6m",
        "interest1y",
        "interest3y",
    ]

    for col in interest_columns:
        df[col] = (
            pd.to_numeric(
                df[col],
                errors="coerce"
            ) / 100
        ).round(5)

    # ========================================================
    # CLEAN + SORT
    # ========================================================

    df = (
        df
        .dropna(
            subset=[
                "time",
                "symbol",
            ]
        )
        .drop_duplicates(
            subset=[
                "time",
                "symbol",
            ],
            keep="last"
        )
        .sort_values(
            [
                "time",
                "symbol",
            ],
            ascending=True
        )
        .reset_index(drop=True)
    )

    # ========================================================
    # DATABASE
    # ========================================================

    engine = create_engine(
        POSTGRES_URL
    )

    # ========================================================
    # CREATE SCHEMA
    # ========================================================

    with engine.begin() as con:
        con.execute(
            text("""
                CREATE SCHEMA IF NOT EXISTS macro
            """)
        )

    # ========================================================
    # CREATE TABLE
    # ========================================================

    with engine.begin() as con:
        con.execute(
            text("""
                CREATE TABLE IF NOT EXISTS macro.bank_interest (
                    "time" date NOT NULL,
                    "symbol" varchar(50) NOT NULL,
                    "bankNameVi" text,
                    "bankNameEn" text,
                    "interest1m" double precision,
                    "interest3m" double precision,
                    "interest6m" double precision,
                    "interest1y" double precision,
                    "interest3y" double precision,

                    PRIMARY KEY (
                        "time",
                        "symbol"
                    )
                )
            """)
        )

    # ========================================================
    # BUILD ROWS
    # ========================================================

    out = df.copy()

    cols = list(out.columns)

    rows = [
        tuple(
            None if pd.isna(x) else x
            for x in row
        )
        for row in out.itertuples(
            index=False,
            name=None
        )
    ]

    if not rows:
        log.info("Không có dữ liệu bank_interest để ghi DB.")
        return 0

    # ========================================================
    # SQL
    # ========================================================

    col_sql = ", ".join(
        f'"{c}"'
        for c in cols
    )

    update_sql = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols
        if c not in [
            "time",
            "symbol",
        ]
    )

    sql = f"""
        INSERT INTO macro.bank_interest
        ({col_sql})
        VALUES %s

        ON CONFLICT (
            "time",
            "symbol"
        )
        DO UPDATE SET
        {update_sql}
    """

    # ========================================================
    # EXECUTE
    # ========================================================

    conn = engine.raw_connection()

    try:

        with conn.cursor() as cur:
            execute_values(
                cur,
                sql,
                rows
            )

        conn.commit()

        now = datetime.now(tz)

        log.info(
            f"✅ Đã upsert {len(rows)} dòng vào "
            f"macro.bank_interest | "
            f"{out['time'].min()} → "
            f"{out['time'].max()} "
            f"lúc {now.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    except Exception:

        conn.rollback()
        raise

    finally:

        conn.close()

    print(
        f"Đã upsert {len(rows)} dòng vào macro.bank_interest."
    )

    return len(rows)


if __name__ == "__main__":
    bank_interest()