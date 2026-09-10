import logging
from datetime import datetime, timedelta

import pytz
import requests
import pandas as pd

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from db_config import POSTGRES_URL

log = logging.getLogger(__name__)


def total_invest():

    # ========================================================
    # CONFIG
    # ========================================================

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://research.aseansc.com.vn/",
        "Origin": "https://research.aseansc.com.vn",
    }

    url = "https://asean-apigw.aseansc.com.vn/pbapi/api/macro/totalinvestment"

    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    # batdau = "01-01-2000"
    today = datetime.now(tz).date()
    batdau = (today - timedelta(days=180)).strftime("%d-%m-%Y")
    kethuc = today.strftime("%d-%m-%Y")

    params = {
        "startDate": batdau,
        "endDate": kethuc,
        "period": "Q",
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
        log.info("API total_invest không có dữ liệu mới.")
        return 0

    # ========================================================
    # DATAFRAME
    # ========================================================

    data = pd.DataFrame(
        json_data["data"]
    )

    required_columns = [
        "reportDate",
        "publicSectorInv",
        "privateSectorInv",
        "FDISectorInv",
        "otherInv",
    ]

    missing_columns = [
        col
        for col in required_columns
        if col not in data.columns
    ]

    if missing_columns:
        raise ValueError(
            f"API total_invest thiếu columns: {missing_columns}"
        )

    df = data[
        required_columns
    ].copy()

    # ========================================================
    # REPORT DATE -> YEAR + QUARTER
    # ========================================================

    df["reportDate"] = pd.to_datetime(
        df["reportDate"],
        errors="coerce"
    )

    df["yearReport"] = (
        df["reportDate"].dt.year
    )

    df["lengtReport"] = (
        df["reportDate"].dt.quarter
    )

    df = df[
        [
            "yearReport",
            "lengtReport",
            "publicSectorInv",
            "privateSectorInv",
            "FDISectorInv",
            "otherInv",
        ]
    ].copy()

    # ========================================================
    # NUMERIC
    # ========================================================

    numeric_columns = [
        "publicSectorInv",
        "privateSectorInv",
        "FDISectorInv",
        "otherInv",
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
        .dropna(
            subset=[
                "yearReport",
                "lengtReport",
            ]
        )
        .drop_duplicates(
            subset=[
                "yearReport",
                "lengtReport",
            ],
            keep="last"
        )
        .sort_values(
            [
                "yearReport",
                "lengtReport",
            ],
            ascending=True
        )
        .reset_index(drop=True)
    )

    df["yearReport"] = (
        df["yearReport"].astype(int)
    )

    df["lengtReport"] = (
        df["lengtReport"].astype(int)
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
                CREATE TABLE IF NOT EXISTS macro.total_invest (
                    "yearReport" integer NOT NULL,
                    "lengtReport" integer NOT NULL,
                    "publicSectorInv" double precision,
                    "privateSectorInv" double precision,
                    "FDISectorInv" double precision,
                    "otherInv" double precision,

                    PRIMARY KEY (
                        "yearReport",
                        "lengtReport"
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
        raise ValueError(
            "Không có dữ liệu total_invest để ghi vào DB."
        )

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
            "yearReport",
            "lengtReport",
        ]
    )

    sql = f"""
        INSERT INTO macro.total_invest
        ({col_sql})
        VALUES %s

        ON CONFLICT (
            "yearReport",
            "lengtReport"
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
            f"macro.total_invest | "
            f"{out['yearReport'].min()} → "
            f"{out['yearReport'].max()} "
            f"lúc {now.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    except Exception:

        conn.rollback()
        raise

    finally:

        conn.close()

    # ========================================================
    # RESULT
    # ========================================================

    print(
        f"Đã upsert {len(rows)} dòng vào macro.total_invest."
    )

    return len(rows)


if __name__ == "__main__":
    total_invest()