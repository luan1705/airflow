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

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
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

def insert_new_index_periods(conn, table_name, rows):
    """
    Chỉ insert các kỳ chưa tồn tại theo:
        yearReport + lengthReport

    Không drop table.
    Không alter table.
    Không update dữ liệu cũ.
    """
    if not rows:
        return 0

    rows = [normalize_row(r) for r in rows]

    # Lấy các kỳ đã có trong DB
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                "yearReport",
                "lengthReport"
            FROM index."{table_name}"
        """)

        existing_rows = cur.fetchall()

    existing_keys = {
        (
            to_float(year_report),
            to_float(length_report),
        )
        for year_report, length_report in existing_rows
    }

    # Chỉ giữ kỳ API có nhưng DB chưa có
    new_rows = []

    for row in rows:
        key = (
            to_float(row.get("yearReport")),
            to_float(row.get("lengthReport")),
        )

        if key not in existing_keys:
            new_rows.append(row)

    if not new_rows:
        return 0

    # Insert kỳ mới
    keys = new_rows[0].keys()

    cols = ",".join(
        f'"{k}"'
        for k in keys
    )

    values = [
        [
            row.get(k)
            for k in keys
        ]
        for row in new_rows
    ]

    sql = f"""
        INSERT INTO index."{table_name}" ({cols})
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(
            cur,
            sql,
            values,
        )

    conn.commit()

    return len(new_rows)


def update_one_index_symbol(symbol):
    """
    Chỉ thêm kỳ mới cho một symbol.
    Không drop bảng.
    Không alter bảng.
    """
    try:
        symbol = symbol.upper()

        url = BASE_URL.format(
            symbol=symbol
        )

        res = requests.get(
            url,
            headers=HEADERS,
            timeout=10,
        )

        if res.status_code != 200:
            return f"[ERROR] {symbol} {res.status_code}"

        data = res.json().get(
            "data",
            [],
        )

        if not data:
            return f"[EMPTY] {symbol}"

        table_name = sanitize_table_name(
            symbol
        )

        with psycopg2.connect(DB_URL) as conn:

            # Kiểm tra bảng đã tồn tại chưa
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'index'
                          AND table_name = %s
                    )
                """, (table_name,))

                table_exists = cur.fetchone()[0]

            # Chưa có bảng:
            # tạo bảng và insert toàn bộ như logic cũ
            if not table_exists:
                create_table_if_not_exists(
                    conn,
                    table_name,
                    data[0],
                )

                insert_data(
                    conn,
                    table_name,
                    data,
                )

                return (
                    f"[OK] {symbol}: "
                    f"tạo mới {len(data)} rows"
                )

            # Đã có bảng:
            # chỉ thêm kỳ mới
            inserted = insert_new_index_periods(
                conn,
                table_name,
                data,
            )

        if inserted == 0:
            log.info(
                f"✅ {symbol}: Không có kỳ mới"
            )

            return (
                f"{symbol}: No new period"
            )

        log.info(
            f"✅ {symbol}: thêm {inserted} kỳ mới"
        )

        return f"{symbol}: +{inserted}"

    except Exception as e:
        log.error(
            f"❌ {symbol}: {e}"
        )

        return (
            f"[FAIL] {symbol}: {e}"
        )


def _run_index_update(symbols):
    print(
        f"🚀 Kiểm tra cập nhật "
        f"{len(symbols)} symbol..."
    )

    results = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=20
    ) as executor:

        futures = {
            executor.submit(
                update_one_index_symbol,
                symbol,
            ): symbol
            for symbol in symbols
        }

        for future in concurrent.futures.as_completed(
            futures
        ):
            symbol = futures[future]

            try:
                results.append(
                    future.result()
                )

            except Exception as exc:
                log.error(
                    f"❌ {symbol}: {exc}"
                )

                results.append(
                    f"[FAIL] {symbol}: {exc}"
                )

    updated = [
        result
        for result in results
        if ": +" in result
    ]

    errors = [
        result
        for result in results
        if (
            "[ERROR]" in result
            or "[EMPTY]" in result
            or "[FAIL]" in result
        )
    ]

    log.info(
        f"✅ Tổng số mã kiểm tra: "
        f"{len(results)}"
    )

    log.info(
        f"🆕 Có kỳ mới: "
        f"{len(updated)} mã"
    )

    log.info(
        f"❌ Tổng số lỗi: "
        f"{len(errors)}"
    )

    if updated:
        log.info(
            "📈 Các mã có dữ liệu mới:"
        )

        for result in updated:
            log.info(result)

    if errors:
        log.warning(
            "📛 Chi tiết các mã lỗi:"
        )

        for result in errors:
            log.warning(result)

    print("✅ Hoàn tất cập nhật!")

    return {
        "total": len(results),
        "updated": len(updated),
        "errors": len(errors),
    }


def index_update():
    """
    Chỉ cập nhật kỳ tài chính mới.

    Không drop bảng.
    Không alter bảng.
    Không sửa kỳ cũ.
    """
    symbols = pd.read_sql(
        text("""
            SELECT symbol
            FROM info.asset
            WHERE exchange IN (
                'HOSE',
                'HNX',
                'UPCOM'
            )
              AND type = 'Stock'
        """),
        engine,
    )["symbol"].tolist()

    return _run_index_update(
        symbols
    )

if __name__ == "__main__":
    index_full()