import logging
from datetime import datetime, timedelta

import pytz
import requests
import pandas as pd

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from db_config import POSTGRES_URL

log = logging.getLogger(__name__)


def exchange_rate():

    # ========================================================
    # CONFIG
    # ========================================================

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://research.aseansc.com.vn/",
        "Origin": "https://research.aseansc.com.vn",
    }

    url = "https://asean-apigw.aseansc.com.vn/pbapi/api/macro/exchangerate"

    # TỰ SỬA THỦ CÔNG KHOẢNG NGÀY Ở ĐÂY
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    # batdau = "01-01-2000"
    today = datetime.now(tz).date()
    batdau = (today - timedelta(days=4)).strftime("%d-%m-%Y")
    kethuc = today.strftime("%d-%m-%Y")

    params = {
        "startDate": batdau,
        "endDate": kethuc,
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
        raise ValueError(
            "API trả về data rỗng."
        )

    # ========================================================
    # DATAFRAME
    # ========================================================

    data = pd.DataFrame(
        json_data["data"]
    )

    required_columns = [
        "reportDate",
        "centerRate",
        "vcbRate",
        "marketRate",
    ]

    missing_columns = [
        col
        for col in required_columns
        if col not in data.columns
    ]

    if missing_columns:
        raise ValueError(
            f"API thiếu columns: {missing_columns}"
        )

    df = data[
        [
            "reportDate",
            "centerRate",
            "vcbRate",
            "marketRate",
        ]
    ].copy()

    # ========================================================
    # Time
    # ========================================================

    df["time"] = pd.to_datetime(
        df["reportDate"],
        errors="coerce"
    ).dt.date

    df = df[
        [
            "time",
            "centerRate",
            "vcbRate",
            "marketRate",
        ]
    ].copy()

    # ========================================================
    # NUMERIC
    # ========================================================

    numeric_columns = [
        "centerRate",
        "vcbRate",
        "marketRate",
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )
    
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

    df[numeric_columns] = df[numeric_columns].ffill()

    # ========================================================
    # DATABASE
    # ========================================================

    engine = create_engine(
        POSTGRES_URL
    )


    # ========================================================
    # CREATE TABLE
    # ========================================================

    with engine.begin() as con:
        con.execute(
            text("""
                CREATE TABLE IF NOT EXISTS macro.exchange_rate (
                    "time" date PRIMARY KEY,
                    "centerRate" double precision,
                    "vcbRate" double precision,
                    "marketRate" double precision
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
        raise ValueError(
            "Không có dữ liệu để ghi vào DB."
        )

    # ========================================================
    # SQL COLUMNS
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

    # ========================================================
    # UPSERT
    # ========================================================

    sql = f"""
        INSERT INTO macro.exchange_rate
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
            f"macro.exchange_rate: "
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
        f"Đã upsert {len(rows)} dòng vào macro.exchange_rate."
    )

    return len(rows)


if __name__ == "__main__":
    exchange_rate()