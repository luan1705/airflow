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
MARKET_INDEX_MAP = {
    "HOSE": "VNINDEX",
    "HNX": "HNXIndex",
    "UPCOM": "UpcomIndex",
}
TYPES = ("pe", "pb")
TIME_FRAME = "SIX_MONTHS"
# TIME_FRAME = "ALL"

URL = "https://trading.vietcap.com.vn/api/iq-insight-service/v1/market-watch/index-valuation"
HEADERS = {
    "content-type": "application/json",
    "referer": "https://trading.vietcap.com.vn/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
}


def fetch_history(index_code: str, metric_type: str) -> pd.DataFrame:
    params = {
        "type": metric_type,
        "comGroupCode": index_code,
        "timeFrame": TIME_FRAME,
    }
    response = requests.get(URL, headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    data = payload.get("data", {})
    values = data.get("values", [])
    if not values:
        raise ValueError(
            f"Khong co du lieu {metric_type.upper()} cho {index_code}. Response: {payload}"
        )

    df = pd.json_normalize(values)
    expected = ["date", "value"]
    missing = set(expected).difference(df.columns)
    if missing:
        raise ValueError(f"Thieu cot bat buoc {missing}. Cac cot hien co: {list(df.columns)}")

    df = df[expected].copy()
    df = df.rename(columns={"value": metric_type})
    # df["plus1SD"] = data.get("plusOneSD")
    # df["plus2SD"] = data.get("plusTwoSD")
    # df["minus1SD"] = data.get("minusOneSD")
    # df["minus2SD"] = data.get("minusTwoSD")
    # df["average"] = data.get("average")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    numeric_cols = [metric_type]#, "plus1SD", "plus2SD", "minus1SD", "minus2SD", "average"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["date", metric_type]).drop_duplicates(subset=["date"])
    ordered_cols = [
        "date",
        metric_type,
        # "plus1SD",
        # "plus2SD",
        # "minus1SD",
        # "minus2SD",
        # "average",
    ]
    df = df[ordered_cols]
    df = df.sort_values("date")
    return df


def save_metric(symbol: str, index_code: str, metric_type: str, enginedb) -> None:
    try:
        logging.info("Lay du lieu %s cho %s (%s)", metric_type.upper(), symbol, index_code)
        df = fetch_history(index_code, metric_type)
        table_name = f"{metric_type}_{symbol}"

        cols = list(df.columns)
        cols_quoted = ",".join(f'"{c}"' for c in cols)
        rows = [tuple(x) for x in df.to_numpy()]

        insert_sql = f"""
            INSERT INTO "{SCHEMA}"."{table_name}" ({cols_quoted})
            VALUES %s
            ON CONFLICT ("date") DO UPDATE SET
            "{metric_type}" = EXCLUDED."{metric_type}"
            /* 
            ,"plus1SD"  = EXCLUDED."plus1SD",
            "plus2SD"  = EXCLUDED."plus2SD",
            "minus1SD" = EXCLUDED."minus1SD",
            "minus2SD" = EXCLUDED."minus2SD",
            "average"  = EXCLUDED."average"
            */
            ;
        """

        with enginedb.begin() as conn:
            cur = conn.connection.cursor()
            try:
                execute_values(cur, insert_sql, rows, page_size=1000)
            finally:
                cur.close()

        logging.info("Da luu %s (%s dong)", table_name, len(df))
    except Exception:
        logging.exception(
            "Loi luu %s cho %s (%s)", metric_type.upper(), symbol, index_code
        )


def ensure_tables(enginedb) -> None:
    with enginedb.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";')
        for metric_type in TYPES:
            for symbol in MARKET_INDEX_MAP:
                table_name = f"{metric_type}_{symbol}"
                ddl = f"""
                    CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table_name}" (
                        "date" DATE PRIMARY KEY,
                        "{metric_type}" DOUBLE PRECISION
                        /*
                        ,"plus1SD" DOUBLE PRECISION,
                        "plus2SD" DOUBLE PRECISION,
                        "minus1SD" DOUBLE PRECISION,
                        "minus2SD" DOUBLE PRECISION,
                        "average" DOUBLE PRECISION
                        */
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
        for metric_type in TYPES:
            for symbol, code in MARKET_INDEX_MAP.items():
                save_metric(symbol, code, metric_type, enginedb)
    finally:
        enginedb.dispose()
        logging.info("Da dong ket noi DB")


# if __name__ == "__main__":
#     main()
