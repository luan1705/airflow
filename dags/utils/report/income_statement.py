import requests
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine, text
import re
import time
import concurrent.futures
import logging

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
SCHEMA = "income_statement"
engine = create_engine(DB_URL)

COLUMNS_TO_EXCLUDE = {'organCode', 'ticker', 'createDate', 'updateDate', 'publicDate'}
TEXT_COLUMNS = {'parent', 'name', 'titleVi'}


def remove_excluded_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.drop(columns=[c for c in df.columns if c in COLUMNS_TO_EXCLUDE], errors='ignore')


def create_table_with_typed_columns(conn, schema: str, table: str, df: pd.DataFrame):
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE'))

    if df.empty:
        conn.execute(text(f'CREATE TABLE "{schema}"."{table}" ("_empty" TEXT)'))
        return

    cols_sql = ', '.join(
        f'"{col}" {"TEXT" if col in TEXT_COLUMNS else "FLOAT"}'
        for col in df.columns
    )
    conn.execute(text(f'CREATE TABLE "{schema}"."{table}" ({cols_sql})'))


def insert_df(conn, schema, table, df):
    if df.empty:
        return
    df = df.where(pd.notnull(df), None)
    cols = ', '.join(f'"{c}"' for c in df.columns)
    placeholders = ', '.join(f':{c}' for c in df.columns)
    conn.execute(
        text(f'INSERT INTO "{schema}"."{table}" ({cols}) VALUES ({placeholders})'),
        df.to_dict(orient='records')
    )


def fetch_income_statement(symbol: str, page: str, max_retries=3):
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/json",
        "origin": "https://iq.vietcap.com.vn",
        "referer": "https://iq.vietcap.com.vn/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }

    url_data   = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol.upper()}/financial-statement?section={page}"
    url_metric = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol.upper()}/financial-statement/metrics"

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            resp_data   = requests.get(url_data,   headers=headers, timeout=15)
            resp_metric = requests.get(url_metric, headers=headers, timeout=15)
            resp_data.raise_for_status()
            resp_metric.raise_for_status()

            json_data   = resp_data.json()
            json_metric = resp_metric.json()

            if "data" not in json_data or not json_data["data"]:
                raise ValueError("No data in response")

            data_q = json_normalize(json_data["data"].get("quarters", []))
            data_y = json_normalize(json_data["data"].get("years", []))
            if data_q.empty and data_y.empty:
                raise ValueError("Empty dataframes (no quarter/year data)")

            data = pd.concat([data_q, data_y], axis=0, ignore_index=True)
            data.sort_values(by=["yearReport", "lengthReport"], ascending=True, inplace=True)

            data = data.replace([float('nan'), 'NaN', 'nan', '', 'null', 'NULL'], None)
            data = data.where(pd.notnull(data), None)
            data = remove_excluded_columns(data)

            metric = json_normalize(json_metric.get("data", {}).get(page, []))
            if not metric.empty:
                metric = metric[["parent", "level", "name", "titleVi"]]
                metric = metric.replace([float('nan'), 'NaN', 'nan', '', 'null', 'NULL'], None)
                metric = metric.where(pd.notnull(metric), None)

            return data, metric

        except Exception as e:
            if attempt >= max_retries:
                log.error(f"❌ {symbol}: Gave up after {attempt} tries. Last error: {e}")
                raise
            wait_time = min(10, 2 * attempt)
            log.warning(f"⚠️ {symbol}: Retry {attempt}/{max_retries}, reason: {e}. Waiting {wait_time}s...")
            time.sleep(wait_time)


def save_one_symbol(symbol: str, page: str):
    try:
        data, metric = fetch_income_statement(symbol, page)
        if data.empty:
            log.warning(f"⚠️ {symbol}: No data")
            return f"{symbol}: No data"

        table_data   = symbol.upper()
        table_metric = f"{symbol.upper()}_METRIC"

        with engine.begin() as conn:
            create_table_with_typed_columns(conn, SCHEMA, table_data, data)
            if not metric.empty:
                create_table_with_typed_columns(conn, SCHEMA, table_metric, metric)
            insert_df(conn, SCHEMA, table_data, data)
            if not metric.empty:
                insert_df(conn, SCHEMA, table_metric, metric)

        log.info(f"✅ {symbol}")
        return f"{symbol}: OK"

    except Exception as e:
        log.error(f"❌ {symbol}: {e}")
        return f"{symbol}: ERROR ({e})"


def _run_symbols(symbols, page):
    print(f"🚀 Bắt đầu xử lý {len(symbols)} symbol...")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(save_one_symbol, s, page): s for s in symbols}
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                log.error(f"❌ {symbol}: {exc}")
                results.append(f"{symbol}: ERROR ({exc})")
            time.sleep(0.3)

    errors = [r for r in results if "ERROR" in r or "No data" in r]

    log.info(f"✅ Tổng số mã xử lý: {len(results)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")
    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    print("✅ Hoàn tất!")
    return errors if errors else "không có lỗi"


def income_statement_full():
    """Chạy lại toàn bộ symbol, ghi đè dữ liệu cũ."""
    page = "INCOME_STATEMENT"
    symbols = pd.read_sql(
        text("""
            SELECT symbol FROM info.asset
            WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
              AND type = 'Stock'
        """),
        engine
    )['symbol'].tolist()
    return _run_symbols(symbols, page)


def income_statement_missing():
    """Chỉ chạy các symbol chưa có bảng trong schema income_statement."""
    page = "INCOME_STATEMENT"

    asset_symbols = set(pd.read_sql(
        text("""
            SELECT symbol FROM info.asset
            WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
              AND type = 'Stock'
        """),
        engine
    )['symbol'].tolist())

    existing_tables = set(pd.read_sql(
        text(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{SCHEMA}' AND table_name NOT LIKE '%_METRIC'
        """),
        engine
    )['table_name'].str.upper().tolist())

    missing_symbols = sorted(asset_symbols - existing_tables)

    if not missing_symbols:
        print("✅ Không có symbol nào thiếu, bỏ qua.")
        return "không có symbol thiếu"

    print(f"🔍 Phát hiện {len(missing_symbols)} symbol thiếu: {missing_symbols}")
    return _run_symbols(missing_symbols, page)


if __name__ == "__main__":
    income_statement_full()