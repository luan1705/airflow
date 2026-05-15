from sqlalchemy import create_engine, text
import pandas as pd
import concurrent.futures
from datetime import datetime
from .ssi_tradingview_1D import ssi_tradingview_1D,get_access_token
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE, indices
import time
import logging
import re
from psycopg2.extras import execute_values
import requests

# Thiết lập logging 
log=logging.getLogger(__name__)

# Kết nối PostgreSQL
engine = create_engine(# method://user:pass@host:port/dbName
                       "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
                        pool_size=10,
                        max_overflow=20,
                        pool_timeout=60
                        )

SCHEMA = "ohlcv_check"

PG_IDENT_MAX = 63

def _qi(name: str) -> str:
    """Quote identifier nếu không phải dạng hợp lệ (hoặc bạn muốn ép quote)."""
    s = str(name)
    # Cho an toàn, ta QUOTE luôn để khỏi nghĩ đến case bắt đầu bằng số, ký tự lạ...
    return '"' + s.replace('"', '""') + '"'

def _quoted_ident(schema: str, table: str) -> str:
    """Schema-qualified identifier với escape dấu nháy kép."""
    return f'{_qi(schema)}.{_qi(table)}'

def _sanitize_symbol_for_table(symbol: str) -> str:
    s = str(symbol).upper()
    if not re.fullmatch(r"[A-Z0-9_]+", s):
        raise ValueError(f"Symbol không hợp lệ để đặt tên bảng: {symbol}")
    return s

def _ensure_table_with_pk(conn, schema: str, table: str):
    """Đảm bảo tồn tại bảng <schema>.<table> và có PRIMARY KEY(time)."""
    fqtn = _quoted_ident(schema, table)
    pk_name = f"{table}_pk"

    # 1) Tạo nếu chưa có (kèm PK)
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {fqtn} (
            symbol   TEXT,
            time     TIMESTAMPTZ          NOT NULL,
            open     DOUBLE PRECISION,
            close    DOUBLE PRECISION,
            high     DOUBLE PRECISION,
            low      DOUBLE PRECISION,
            volume   DOUBLE PRECISION,
            CONSTRAINT {_qi(pk_name)} PRIMARY KEY (time)
        );
    """))

    # 2) Nếu đã có bảng nhưng thiếu PK(time) -> thêm
    res = conn.execute(text("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = :schema
          AND tc.table_name   = :table
          AND tc.constraint_type = 'PRIMARY KEY';
    """), {"schema": schema, "table": table}).fetchall()

    pk_cols = {r[0] for r in res}
    if pk_cols != {"time"}:
        try:
            conn.execute(text(f'ALTER TABLE {fqtn} ADD CONSTRAINT {_qi(pk_name)} PRIMARY KEY (time);'))
        except Exception as e:
            # Có thể đã có PK tên khác / dữ liệu trùng -> log để biết
            log.warning(f"Không thể ADD PRIMARY KEY cho {schema}.{table}: {e}")
    elif pk_cols != {"time"}:
        log.warning(f"{schema}.{table} đang có PK khác: {pk_cols}")




# Hàm cập nhật dữ liệu cho một mã cổ phiếu
def get_stock(symbol,token):
    max_retries = 3
    stock = pd.DataFrame()
    try:
        for attempt in range(max_retries):
            try:
                time.sleep(0.5)  # giảm delay

                stock = ssi_tradingview_1D(symbol=symbol, token=token)

                if stock is not None and not stock.empty and "time" in stock.columns:
                    break

            except (requests.exceptions.RequestException, ValueError) as e:
                log.warning(f"[{symbol}] attempt {attempt+1} error: {e}")

            time.sleep(1.5 * (attempt + 1))

        if stock is None or stock.empty or "time" not in stock.columns:
            return f"❌ FAIL: {symbol}"
        
        symbol = 'UPCOMINDEX' if symbol == 'HNXUpcomIndex' else symbol
        symbol = 'HNXINDEX' if symbol == 'HNXIndex' else symbol

            
        stock = stock.copy()
        # stock['exchange'] = exch
        stock['symbol'] = _sanitize_symbol_for_table(symbol)
        stock['time'] = (pd.to_datetime(stock['time'])+ pd.Timedelta(hours=15)).dt.floor('s')

        # Chỉ giữ các cột phù hợp schema đã khai báo (thêm/bớt theo thực tế DataFrame bạn trả về)
        keep_cols = [c for c in ['symbol','time','open','close','high','low','volume'] if c in stock.columns]
        stock = stock[keep_cols].drop_duplicates(subset=['time'])

        # Tên bảng
        table_name = f"{_sanitize_symbol_for_table(symbol)}_1D"
        
        # Ghi vào PostgreSQL
        with engine.begin() as conn:
            # tạo bảng
            _ensure_table_with_pk(conn, SCHEMA, table_name)

            # 👇 xoá sạch bảng (REPLACE)
            conn.execute(text(f"TRUNCATE TABLE {_quoted_ident(SCHEMA, table_name)}"))

            rows = [tuple(x) for x in stock.to_numpy()]
            cols = ', '.join([_qi(c) for c in stock.columns])

            insert_sql = f"""
                INSERT INTO {_quoted_ident(SCHEMA, table_name)} ({cols})
                VALUES %s
            """

            with conn.connection.cursor() as cur:
                execute_values(cur, insert_sql, rows, page_size=1000)
        
            msg = f"✅ Đã lưu {symbol}"
            log.info(msg)
            return msg

    except Exception as e:
        msg = f"❌ Lỗi khi xử lý {symbol}: {str(e)}"
        log.error(msg)
        return msg

# Cập nhật toàn bộ danh sách mã cho một sàn
def update_all_stocks(symbol_list,token):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(get_stock, symbol, token) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.append(future.result())
    return messages

# test = ['SMB']
# Hàm tổng cho tất cả sàn — để DAG gọi
def tradingview_1D():
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    token = get_access_token()
    result = []
    result += update_all_stocks(HOSE, token)
    result += update_all_stocks(HNX, token)
    result += update_all_stocks(UPCOM, token)
    result += update_all_stocks(DERIVATIVES, token)
    result += update_all_stocks(CW, token)
    result += update_all_stocks(HNXBOND, token)
    result += update_all_stocks(ETFHOSE, token)
    result += update_all_stocks(indices, token)
    # result += update_all_stocks(test, token)

    errors = [msg for msg in result if msg.startswith("❌") or msg.startswith("⚠️")]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    # if len(errors) >= 5:
    #     raise Exception("Task thất bại vì có lỗi:\n" + "\n".join(errors))

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return errors if errors else "Không có lỗi."