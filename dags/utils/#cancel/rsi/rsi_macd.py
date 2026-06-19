import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

SCHEMA = "exchange_history"
MARKET_INDEX_MAP = {
    "HOSE": "VNINDEX",
    "HNX": "HNXINDEX",
    "UPCOM": "UPCOMINDEX",
}
CONDITIONS = ("RSI50", "RSI70", "MACD0")
DATE_RANGE_DAYS = 182

URL = "https://backend.miquant.vn/api/v1/market/metric/data"
HEADERS = {
    "content-type": "application/json",
    "referer": "https://app.miquant.vn/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
}
CONDITION_CONFIG = {
    "RSI50": {
        "category_code": "MARKET_BREADTH_IDX",
        "series_key": "pct_above_rsi50",
        "chart_code": "MARKET_BREADTH_IDX_5",
    },
    "RSI70": {
        "category_code": "MACRO_RISK_IDX",
        "series_key": "rsi_above_70_pct",
        "chart_code": "MACRO_RISK_IDX_1",
    },
    "MACD0": {
        "category_code": "MARKET_BREADTH_IDX",
        "series_key": "macd_hist_positive_pct",
        "chart_code": "MARKET_BREADTH_IDX_6",
    },
}


def table_name_for_condition(condition: str, symbol: str) -> str:
    if condition.startswith("RSI"):
        period = condition.replace("RSI", "")
        return f"rsi{period}_{symbol}"
    if condition.startswith("MACD"):
        period = condition.replace("MACD", "")
        return f"macd{period}_{symbol}"
    raise ValueError(f"Condition khong ho tro: {condition}")


def fetch_indicator(index_code: str, condition: str) -> pd.DataFrame:
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=DATE_RANGE_DAYS)
    config = CONDITION_CONFIG[condition]

    params = {
        "categoryCode": config["category_code"],
        "symbol": index_code,
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "isCumulativeList": "false",
        "seriesKeys": config["series_key"],
        "chartCode": config["chart_code"],
    }
    response = requests.get(URL, headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    data = payload.get("data", {})
    timestamps = data.get("timestamps", [])
    metric_data = data.get("metricData", [])
    if not timestamps or not metric_data:
        raise ValueError(
            f"Khong co du lieu {condition} cho {index_code}. Response: {payload}"
        )

    values = metric_data[0].get("data", [])
    if len(values) != len(timestamps):
        raise ValueError(
            f"So luong timestamp ({len(timestamps)}) khong khop so luong gia tri ({len(values)}) cho {index_code} - {condition}"
        )

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(timestamps, unit="ms").date,
            "condition": condition,
            "percent": values,
        }
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["percent"] = pd.to_numeric(df["percent"], errors="coerce")
    df = df.dropna(subset=["date", "condition", "percent"])
    df = df.drop_duplicates(subset=["date"])
    df = df.sort_values("date")
    return df


def save_indicator(symbol: str, index_code: str, condition: str, enginedb) -> None:
    try:
        logging.info("Lay du lieu %s cho %s (%s)", condition, symbol, index_code)
        df = fetch_indicator(index_code, condition)
        table_name = table_name_for_condition(condition, symbol)
        cols = list(df.columns)
        cols_quoted = ",".join(f'"{c}"' for c in cols)
        rows = [tuple(x) for x in df.to_numpy()]

        insert_sql = f"""
            INSERT INTO "{SCHEMA}"."{table_name}" ({cols_quoted})
            VALUES %s
            ON CONFLICT ("date") DO UPDATE SET
            "condition" = EXCLUDED."condition",
            "percent"   = EXCLUDED."percent";
        """

        with enginedb.begin() as conn:
            cur = conn.connection.cursor()
            try:
                execute_values(cur, insert_sql, rows, page_size=1000)
            finally:
                cur.close()

        logging.info("Da luu %s (%s dong)", table_name, len(df))
    except Exception:
        logging.exception("Loi luu %s cho %s (%s)", condition, symbol, index_code)


def ensure_tables(enginedb) -> None:
    with enginedb.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";')
        for symbol in MARKET_INDEX_MAP:
            for condition in CONDITIONS:
                table_name = table_name_for_condition(condition, symbol)
                ddl = f"""
                    CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table_name}" (
                        "date" DATE PRIMARY KEY,
                        "condition" TEXT,
                        "percent" DOUBLE PRECISION
                    );
                """
                conn.exec_driver_sql(ddl)
                logging.info("Da dam bao ton tai bang %s.%s", SCHEMA, table_name)


def main() -> None:
    enginedb = create_engine(
        "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
    )
    try:
        logging.info("Ket noi DB thanh cong")
        ensure_tables(enginedb)
        for symbol, index_code in MARKET_INDEX_MAP.items():
            for condition in CONDITIONS:
                save_indicator(symbol, index_code, condition, enginedb)
    finally:
        enginedb.dispose()
        logging.info("Da dong ket noi DB")


if __name__ == "__main__":
    main()
