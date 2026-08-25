from sqlalchemy import create_engine, text
import pandas as pd
import concurrent.futures
from datetime import datetime
from .ssi_tradingview_1D_v3 import ssi_tradingview_1D, get_access_token
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE, indices, custom_list
import time
import logging
import re
from psycopg2.extras import execute_values
import requests

# Thiết lập logging 
log=logging.getLogger(__name__)

# Kết nối PostgreSQL
engine = create_engine(# method://user:pass@host:port/dbName
                       "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
                        pool_size=10,
                        max_overflow=20,
                        pool_timeout=60
                        )

SCHEMA = "ohlcv"

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
            value    DOUBLE PRECISION,
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
    if not pk_cols:
        try:
            conn.execute(text(f'ALTER TABLE {fqtn} ADD CONSTRAINT {_qi(pk_name)} PRIMARY KEY (time);'))
        except Exception as e:
            # Có thể đã có PK tên khác / dữ liệu trùng -> log để biết
            log.warning(f"Không thể ADD PRIMARY KEY cho {schema}.{table}: {e}")
    elif pk_cols != {"time"}:
        log.warning(f"{schema}.{table} đang có PK khác: {pk_cols}")




# Hàm cập nhật dữ liệu cho một mã cổ phiếu
def get_stock(symbol,token):
    try:
        time.sleep(1)
        today = datetime.now().strftime('%Y-%m-%d')

        # Lấy dữ liệu lịch sử
        stock = ssi_tradingview_1D(symbol=symbol, token=token)
        # Chuẩn hóa exchange
        # exch = (
        #     'HOSE' if symbol in HOSE else
        #     'HNX' if symbol in HNX else
        #     'UPCOM' if symbol in UPCOM else
        #     'DERIVATIVES' if symbol in DERIVATIVES else
        #     'CW' if symbol in CW else
        #     'HNXBOND' if symbol in HNXBOND else
        #     'ETFHOSE' if symbol in ETFHOSE else
        #     'INDICES'
        # )
        # Kiểm tra dữ liệu trả về
        if stock is None or stock.empty:
            msg = f"⚠️ Không có dữ liệu cho {symbol}"
            log.warning(msg)
            return {"symbol": symbol,"status": "warning","message": msg}
        # Lọc bỏ volume = 0 và volume = NaN
        if 'volume' in stock.columns:
            stock = stock[
                stock['volume'].notna() &
                (stock['volume'] != 0)
            ].copy()

            if stock.empty:
                msg = f"⚠️ Sau khi lọc volume=0/NaN thì không còn dữ liệu cho {symbol}"
                log.warning(msg)
                return {
                    "symbol": symbol,
                    "status": "warning",
                    "message": msg
                }

        symbol = 'UPCOMINDEX' if symbol == 'HNXUpcomIndex' else symbol
        symbol = 'HNXINDEX' if symbol == 'HNXIndex' else symbol

        # Ép kiểu time -> DATE (yyyy-mm-dd), loại bỏ trùng theo time
        if 'time' not in stock.columns:
            raise ValueError("DataFrame thiếu cột 'time'")
            
        stock = stock.copy()
        # stock['exchange'] = exch
        stock['symbol'] = _sanitize_symbol_for_table(symbol)
        stock['time'] = (pd.to_datetime(stock['time'])+ pd.Timedelta(hours=15)).dt.floor('s')

        # Chỉ giữ các cột phù hợp schema đã khai báo (thêm/bớt theo thực tế DataFrame bạn trả về)
        keep_cols = [c for c in ['symbol','time','open','close','high','low','volume','value'] if c in stock.columns]
        stock = stock[keep_cols].drop_duplicates(subset=['time'])

        # Tên bảng
        table_name = f"{_sanitize_symbol_for_table(symbol)}_1D"
        
        # Ghi vào PostgreSQL
        with engine.begin() as conn:
            # Tạo bảng (nếu chưa có) với PK(time)
            _ensure_table_with_pk(conn, SCHEMA, table_name)

            # --- Chèn dữ liệu mới, nếu time đã tồn tại thì bỏ qua ---
            rows = [tuple(x) for x in stock.to_numpy()]
            cols = ', '.join([_qi(c) for c in stock.columns])
            insert_sql = f"""
                INSERT INTO {_quoted_ident(SCHEMA, table_name)} ({cols})
                VALUES %s
                ON CONFLICT (time) DO UPDATE SET
                    symbol   = EXCLUDED.symbol,
                    open     = EXCLUDED.open,
                    close    = EXCLUDED.close,
                    high     = EXCLUDED.high,
                    low      = EXCLUDED.low,
                    volume   = EXCLUDED.volume,
                    value    = EXCLUDED.value;
            """
            execute_values(conn.connection.cursor(), insert_sql, rows, page_size=1000)
    
        msg = f"✅ Đã lưu {symbol}"
        log.info(msg)
        return {"symbol": symbol,"status": "success","message": msg}

    except requests.HTTPError as e:

        # SSI trả 401 -> token không còn hợp lệ
        if (e.response is not None and e.response.status_code == 401):
            log.warning(f"🔑 401 Unauthorized tại {symbol}")

            return {"symbol": symbol,"status": "token_expired","message": f"🔑 Token hết hạn tại {symbol}"}

        msg = f"❌ Lỗi khi xử lý {symbol}: {str(e)}"

        log.error(msg)

        return {"symbol": symbol,"status": "error","message": msg}

    except Exception as e:
        msg = f"❌ Lỗi khi xử lý {symbol}: {str(e)}"
        log.error(msg)
        return {"symbol": symbol,"status": "error","message": msg}

# Cập nhật toàn bộ danh sách mã cho một sàn
def update_all_stocks(symbol_list,token):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_stock, symbol, token):symbol for symbol in symbol_list}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            messages.append(result)
    token_expired_symbols = [item["symbol"]for item in messages if item["status"] == "token_expired"]
    if token_expired_symbols:
        log.warning(f"🔄 Phát hiện {len(token_expired_symbols)} mã bị 401. Đang refresh access token...")
        token = get_access_token()
        log.info("✅ Đã lấy access token mới")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_stock,symbol,token) for symbol in token_expired_symbols]
            retry_results = [future.result() for future in concurrent.futures.as_completed(futures)]
        messages = [item for item in messages if item["status"] != "token_expired"]
        messages.extend(retry_results)
        for item in messages:
            if item["status"] == "token_expired":
                item["status"] = "error"
                item["message"] = f"❌ {item['symbol']}: 401 sau khi refresh token"
    return messages, token


# Hàm tổng cho tất cả sàn — để DAG gọi
def tradingview_1D():
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    token = get_access_token()
    result = []
    messages, token = update_all_stocks(HOSE, token)
    result += messages
    messages, token = update_all_stocks(HNX, token)
    result += messages
    messages, token = update_all_stocks(UPCOM, token)
    result += messages
    messages, token = update_all_stocks(DERIVATIVES, token)
    result += messages
    messages, token = update_all_stocks(CW, token)
    result += messages
    messages, token = update_all_stocks(HNXBOND, token)
    result += messages
    messages, token = update_all_stocks(ETFHOSE, token)
    result += messages
    messages, token = update_all_stocks(indices, token)
    result += messages
    # messages, token = update_all_stocks(custom_list, token)
    # result += messages

    errors = [item for item in result if item["status"] in ["error","warning"]]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for item in errors:
            log.warning(item["message"])

    # if len(errors) >= 5:
    #     raise Exception("Task thất bại vì có lỗi:\n" + "\n".join(errors))

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return ([item["message"] for item in errors] if errors else "không có lỗi")