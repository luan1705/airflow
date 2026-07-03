import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import re
import time
import concurrent.futures
import logging
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

DB_URL = "postgresql://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)

BASE_URL = "https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}/statistics-financial"

HEADERS = {
    "accept": "application/json",
    "origin": "https://trading.vietcap.com.vn",
    "referer": "https://trading.vietcap.com.vn/",
    "user-agent": "Mozilla/5.0"
}


def sanitize_table_name(symbol):
    return re.sub(r'[^A-Z0-9_]', '', symbol.upper())


def to_float(value):
    try:
        return float(value)
    except:
        return None


def normalize_row(row):
    new_row = {}
    for k, v in row.items():
        val = to_float(v)
        if val is not None:
            if k in ["roa", "roe", "pe", "pb"] and val == 0:
                new_row[k] = None
            else:
                new_row[k] = val
        else:
            new_row[k] = v
    return new_row


def create_table_if_not_exists(conn, table_name, sample_row):
    cols = []
    for k, v in sample_row.items():
        try:
            float(v)
            t = "DOUBLE PRECISION"
        except:
            t = "TEXT"
        cols.append(f'"{k}" {t}')

    sql = f'CREATE TABLE IF NOT EXISTS index."{table_name}" ({",".join(cols)});'

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def insert_data(conn, table_name, rows):
    rows = [normalize_row(r) for r in rows]
    keys = rows[0].keys()
    cols = ','.join([f'"{k}"' for k in keys])
    values = [[row.get(k) for k in keys] for row in rows]

    sql = f'INSERT INTO index."{table_name}" ({cols}) VALUES %s'

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()


def process_symbol(symbol):
    try:
        url = BASE_URL.format(symbol=symbol)
        res = requests.get(url, headers=HEADERS, timeout=10)

        if res.status_code != 200:
            return f"[ERROR] {symbol} {res.status_code}"

        data = res.json().get("data", [])
        if not data:
            return f"[EMPTY] {symbol}"

        table_name = sanitize_table_name(symbol)

        with psycopg2.connect(DB_URL) as conn:
            create_table_if_not_exists(conn, table_name, data[0])
            insert_data(conn, table_name, data)

        return f"[OK] {symbol} ({len(data)} rows)"

    except Exception as e:
        return f"[FAIL] {symbol}: {e}"


def _run_symbols(symbols):
    print(f"🚀 Bắt đầu xử lý {len(symbols)} symbol...")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_symbol, s): s for s in symbols}
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                log.error(f"❌ {symbol}: {exc}")
                results.append(f"[FAIL] {symbol}: {exc}")

    errors = [r for r in results if "ERROR" in r or "EMPTY" in r or "FAIL" in r]

    log.info(f"✅ Tổng số mã xử lý: {len(results)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")
    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    print("✅ Hoàn tất!")
    return errors if errors else "không có lỗi"


def index_full():
    """Chạy lại toàn bộ symbol."""
    symbols = pd.read_sql(
        text("""
            SELECT symbol FROM info.asset
            WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
              AND type = 'Stock'
        """),
        engine
    )['symbol'].tolist()
    return _run_symbols(symbols)


def index_missing():
    """Chỉ chạy các symbol chưa có bảng trong schema index."""
    asset_symbols = set(pd.read_sql(
        text("""
            SELECT symbol FROM info.asset
            WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
              AND type = 'Stock'
        """),
        engine
    )['symbol'].tolist())

    existing_tables = set(pd.read_sql(
        text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'index'
        """),
        engine
    )['table_name'].str.upper().tolist())

    missing_symbols = sorted(asset_symbols - existing_tables)

    if not missing_symbols:
        print("✅ Không có symbol nào thiếu, bỏ qua.")
        return "không có symbol thiếu"

    print(f"🔍 Phát hiện {len(missing_symbols)} symbol thiếu: {missing_symbols}")
    return _run_symbols(missing_symbols)


if __name__ == "__main__":
    index_full()