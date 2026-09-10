import logging
from datetime import datetime, timedelta

import pytz
import requests
import pandas as pd

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from .interbank_interest_on import batdau, kethuc
from db_config import POSTGRES_URL

log = logging.getLogger(__name__)


def interbank_interest():

    # ========================================================
    # CONFIG
    # ========================================================

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://research.aseansc.com.vn/",
        "Origin": "https://research.aseansc.com.vn",
    }

    url = "https://asean-apigw.aseansc.com.vn/pbapi/api/macro/interbankinterest"

    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    # batdau = "01-01-2000"
    # today = datetime.now(tz).date()
    # # batdau = (today - timedelta(days=4)).strftime("%d-%m-%Y")
    # kethuc = today.strftime("%d-%m-%Y")

    params = {
        "startDate": batdau,
        "endDate": kethuc,
        "interestperiod": "3M"
    }

    # ========================================================
    # CALL API
    # ========================================================

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
        log.info("API interbank_interest không có dữ liệu mới.")
        return 0

    # ========================================================
    # DATAFRAME
    # ========================================================

    data = pd.DataFrame(
        json_data["data"]
    )

    required_columns = [
        "reportDate",
        "interest",
    ]

    missing_columns = [
        col
        for col in required_columns
        if col not in data.columns
    ]

    if missing_columns:
        raise ValueError(
            f"API interbank_interest thiếu columns: {missing_columns}"
        )

    df = data[
        required_columns
    ].copy()

    # ========================================================
    # TIME
    # ========================================================

    df["time"] = pd.to_datetime(
        df["reportDate"],
        errors="coerce"
    ).dt.date

    # 4.91% -> 0.0491
    df["3m"] = (
        pd.to_numeric(
            df["interest"],
            errors="coerce"
        ).astype(float)
        / 100
    ).round(5)

    df = df[
        [
            "time",
            "3m",
        ]
    ].copy()

    # ========================================================
    # CLEAN + SORT
    # ========================================================

    df = (
        df
        .dropna(subset=["time"])
        .drop_duplicates(
            subset=["time"],
            keep="last"
        )
        .sort_values(
            "time",
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
    # CREATE column
    # ========================================================

    with engine.begin() as con:
        con.execute(
            text("""
                ALTER TABLE macro.interbank_interest
                ADD COLUMN IF NOT EXISTS "3m" double precision
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
        log.info("Không có dữ liệu interbank_interest để ghi DB.")
        return 0

    # ========================================================
    # UPSERT SQL
    # ========================================================

    col_sql = ", ".join(
        f'"{c}"'
        for c in cols
    )

    update_sql = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols
        if c != "time"
    )

    sql = f"""
        INSERT INTO macro.interbank_interest
        ({col_sql})
        VALUES %s

        ON CONFLICT ("time")
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
            f"macro.interbank_interest | "
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
        f"Đã upsert {len(rows)} dòng vào macro.interbank_interest."
    )

    return len(rows)


if __name__ == "__main__":
    interbank_interest()