import logging

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
EXCHANGE_MAP = {
    "HOSE": "HSX",
    "HNX": "HNX",
    "UPCOM": "UPCOM",
}
CONDITIONS = ("EMA20", "EMA50", "EMA100", "EMA200")
NUMBER_OF_DAYS = "M6"

URL = "https://iq.vietcap.com.vn/api/iq-insight-service/v1/market-watch/breadth"
HEADERS = {
    "content-type": "application/json",
    "referer": "https://trading.vietcap.com.vn/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
}


def fetch_breadth(exchange_code: str, condition: str) -> pd.DataFrame:
    params = {
        "condition": condition,
        "exchange": exchange_code,
        "enNumberOfDays": NUMBER_OF_DAYS,
    }
    response = requests.get(URL, headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    records = payload.get("data", [])
    if not records:
        raise ValueError(
            f"Khong co du lieu breadth cho exchange={exchange_code}, condition={condition}. Response: {payload}"
        )

    df = pd.json_normalize(records)
    rename_map = {"tradingDate": "date"}
    df = df.rename(columns=rename_map)

    expected = ["condition", "date", "count", "total", "percent"]
    missing = set(expected).difference(df.columns)
    if missing:
        raise ValueError(f"Thieu cot bat buoc {missing}. Cac cot hien co: {list(df.columns)}")

    df = df[expected].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for col in ["count", "total", "percent"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["date", "condition", "count", "total", "percent"])
    df = df.drop_duplicates(subset=["date"])
    df = df.sort_values("date")
    return df


def save_breadth(symbol: str, exchange_code: str, condition: str, enginedb) -> None:
    try:
        logging.info("Lay du lieu breadth cho %s (%s) - %s", symbol, exchange_code, condition)
        df = fetch_breadth(exchange_code, condition)

        period = condition.replace("EMA", "")
        table_name = f"breadth{period}_{symbol}"
        cols = list(df.columns)
        cols_quoted = ",".join(f'"{c}"' for c in cols)
        rows = [tuple(x) for x in df.to_numpy()]

        insert_sql = f"""
            INSERT INTO "{SCHEMA}"."{table_name}" ({cols_quoted})
            VALUES %s
            ON CONFLICT ("date") DO UPDATE SET
            "condition" = EXCLUDED."condition",
            "count"     = EXCLUDED."count",
            "total"     = EXCLUDED."total",
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
        logging.exception("Loi luu breadth cho %s (%s) - %s", symbol, exchange_code, condition)


def ensure_tables(enginedb) -> None:
    with enginedb.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";')
        for symbol in EXCHANGE_MAP:
            for condition in CONDITIONS:
                period = condition.replace("EMA", "")
                table_name = f"breadth{period}_{symbol}"
                ddl = f"""
                    CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table_name}" (
                        "date" DATE PRIMARY KEY,
                        "condition" TEXT,
                        "count" DOUBLE PRECISION,
                        "total" DOUBLE PRECISION,
                        "percent" DOUBLE PRECISION
                    );
                """
                conn.exec_driver_sql(ddl)
                logging.info("Da dam bao ton tai bang %s.%s", SCHEMA, table_name)


def main() -> None:
    enginedb = create_engine(
        "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
    )
    try:
        logging.info("Ket noi DB thanh cong")
        ensure_tables(enginedb)
        for symbol, exchange_code in EXCHANGE_MAP.items():
            for condition in CONDITIONS:
                save_breadth(symbol, exchange_code, condition, enginedb)
    finally:
        enginedb.dispose()
        logging.info("Da dong ket noi DB")


if __name__ == "__main__":
    main()
