import time
import logging
from datetime import datetime

import pytz
import requests
import pandas as pd
import numpy as np

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs


# ============================================================
# NumPy adapter
# ============================================================

register_adapter(np.float64, lambda v: AsIs(float(v)))
register_adapter(np.int64,   lambda v: AsIs(int(v)))


log = logging.getLogger(__name__)


# ============================================================
# INVESTOR CAPITAL
# ============================================================

def invest_capital(n_days=3):

    # ========================================================
    # API
    # ========================================================

    url = (
        "https://api-finance-t19.24hmoney.vn/v1/ios/"
        "stock/statistic-investor-history"
    )

    params = {
    # "device_id": "web1780903gbsz5wuwfbj0j6dkwh0pwpqnms04mypi735539",
    # "device_name": "INVALID",
    # "device_model": "Windows 11",
    # "network_carrier": "INVALID",
    # "connection_type": "INVALID",
    # "os": "Chrome",
    # "os_version": "151.0.0.0",
    # "access_token": "INVALID",
    # "push_token": "INVALID",
    # "locale": "vi",
    # "browser_id": "web1780903gbsz5wuwfbj0j6dkwh0pwpqnms04mypi735539",
    "symbol": 10,
}


    # ========================================================
    # Timezone
    # ========================================================

    tz = pytz.timezone("Asia/Ho_Chi_Minh")

    # ========================================================
    # CALL API
    # ========================================================

    while True:

        today = datetime.now(tz).date()

        try:

            response = requests.get(
                url,
                params=params,
                timeout=30
            )

            response.raise_for_status()

            json_data = response.json()

            if "data" not in json_data:
                raise ValueError(
                    f"API không có key 'data': {json_data}"
                )

            data = json_data["data"]

            if not data:
                raise ValueError(
                    "API trả về data rỗng."
                )

            # =================================================
            # RAW DATAFRAME
            # =================================================

            df = pd.DataFrame(data)

            # =================================================
            # CHECK REQUIRED COLUMNS
            # =================================================

            required_columns = [
                "trading_date",

                "proprietary_buy",
                "proprietary_sell",

                "local_individual_buy",
                "local_individual_sell",

                "local_institutional_buy",
                "local_institutional_sell",

                "foreign_individual_buy",
                "foreign_individual_sell",

                "foreign_institutional_buy",
                "foreign_institutional_sell",
            ]

            missing_columns = [
                col
                for col in required_columns
                if col not in df.columns
            ]

            if missing_columns:
                raise ValueError(
                    f"API thiếu columns: {missing_columns}"
                )

            # =================================================
            # PARSE DATE
            #
            # trading_date là Unix timestamp SECONDS
            #
            # Ví dụ:
            # 1787677200 -> 2026-08-25
            # =================================================

            df["date"] = (
                pd.to_datetime(
                    df["trading_date"],
                    unit="s",
                    utc=True
                )
                .dt.tz_convert("Asia/Ho_Chi_Minh")
                .dt.date
            )

            # =================================================
            # CHECK API HAS TODAY DATA
            # =================================================

            api_dates = set(
                df["date"].dropna()
            )

            if today in api_dates:

                log.info(
                    f"✅ Đã có dữ liệu invest_capital "
                    f"ngày {today}."
                )

                break

            log.info(
                f"⏳ Chưa có dữ liệu invest_capital "
                f"ngày {today}. "
                f"Thử lại sau 5 phút..."
            )

        except Exception as e:

            log.warning(
                f"⚠️ Lỗi gọi API invest_capital: {e}. "
                f"Thử lại sau 5 phút..."
            )

        time.sleep(300)

    # ========================================================
    # NET FLOW
    #
    # QUAN TRỌNG:
    #
    # net = buy - sell
    #
    # KHÔNG dùng:
    # buy_matched - sell_matched
    #
    # vì bảng aggregate cũ đang khớp với buy - sell.
    # ========================================================

    df["netProprietary"] = (
        df["proprietary_buy"]
        - df["proprietary_sell"]
    )

    df["netDomesticIndividual"] = (
        df["local_individual_buy"]
        - df["local_individual_sell"]
    )

    df["netDomesticInstitution"] = (
        df["local_institutional_buy"]
        - df["local_institutional_sell"]
    )

    df["netForeignIndividual"] = (
        df["foreign_individual_buy"]
        - df["foreign_individual_sell"]
    )

    df["netForeignInstitution"] = (
        df["foreign_institutional_buy"]
        - df["foreign_institutional_sell"]
    )

    # ========================================================
    # AGGREGATE
    # ========================================================

    df["totalIndividual"] = (
        df["netDomesticIndividual"]
        + df["netForeignIndividual"]
    )

    df["totalInstitution"] = (
        df["netDomesticInstitution"]
        + df["netForeignInstitution"]
    )

    # ========================================================
    # OUTPUT TABLE
    # ========================================================

    table = df[
        [
            "date",
            "netProprietary",
            "netDomesticIndividual",
            "netDomesticInstitution",
            "netForeignIndividual",
            "netForeignInstitution",
            "totalIndividual",
            "totalInstitution",
        ]
    ].copy()

    # ========================================================
    # SORT
    # ========================================================

    table = (
        table
        .sort_values(
            "date",
            ascending=False
        )
        .reset_index(drop=True)
    )

    # ========================================================
    # DATABASE
    # ========================================================

    engine = create_engine(
        "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
    )

    # ========================================================
    # CREATE SCHEMA
    # ========================================================

    with engine.begin() as con:

        con.execute(
            text(
                """
                CREATE SCHEMA IF NOT EXISTS exchange_history
                """
            )
        )

    # ========================================================
    # CREATE TABLE
    # ========================================================

    with engine.begin() as con:

        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS exchange_history.invest_capital (
                    date date PRIMARY KEY,
                    "netProprietary" double precision,
                    "netDomesticIndividual" double precision,
                    "netDomesticInstitution" double precision,
                    "netForeignIndividual" double precision,
                    "netForeignInstitution" double precision,
                    "totalIndividual" double precision,
                    "totalInstitution" double precision
                )
                """
            )
        )

    # ========================================================
    # SELECT N LATEST DAYS
    # ========================================================

    out = table.head(n_days).copy()

    # ========================================================
    # BUILD ROWS
    # ========================================================

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

    # ========================================================
    # SQL COLUMN
    # ========================================================

    col_sql = ", ".join(
        f'"{c}"'
        for c in cols
    )

    # ========================================================
    # SQL UPDATE
    # ========================================================

    update_sql = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols
        if c != "date"
    )

    # ========================================================
    # UPSERT SQL
    # ========================================================

    sql = f"""
        INSERT INTO exchange_history.invest_capital
        ({col_sql})
        VALUES %s

        ON CONFLICT (date)
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
            f"exchange_history.invest_capital: "
            f"{out['date'].min()} → "
            f"{out['date'].max()} "
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
        f"Đã upsert {len(rows)} dòng."
    )

    return len(rows)