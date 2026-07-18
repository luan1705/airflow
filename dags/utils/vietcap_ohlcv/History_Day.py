from sqlalchemy import create_engine, text
import pandas as pd
import concurrent.futures
from datetime import datetime
from .tradingview import tradingview
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE, indices, custom_list,total_list
import time
import logging
import re

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
            time     TIMESTAMP          NOT NULL,
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
def get_stock(symbol):
    try:
        time.sleep(1)
        today = datetime.now().strftime('%Y-%m-%d')

        # Lấy dữ liệu lịch sử
        stock = tradingview(symbol=symbol, start='2000-01-01', end=today, time='days')
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
            return msg
        
        stock = stock[stock["volume"] != 0].copy()

        if stock.empty:
            msg = f"⚠️ Sau khi lọc volume=0 thì không còn dữ liệu cho {symbol}"
            log.warning(msg)
            return msg

        symbol = 'UPCOMINDEX' if symbol == 'HNXUpcomIndex' else symbol
        symbol = 'HNXINDEX' if symbol == 'HNXIndex' else symbol

        # Ép kiểu time -> DATE (yyyy-mm-dd), loại bỏ trùng theo time
        if 'time' not in stock.columns:
            raise ValueError("DataFrame thiếu cột 'time'")
        stock = stock.copy()
        # stock['exchange'] = exch
        stock['symbol'] = _sanitize_symbol_for_table(symbol)
        stock['time'] = (pd.to_datetime(stock['time']) + pd.Timedelta(hours=15)).dt.floor('s')

        # Chỉ giữ các cột phù hợp schema đã khai báo (thêm/bớt theo thực tế DataFrame bạn trả về)
        keep_cols = [c for c in ['symbol','time','open','close','high','low','volume','value','exchange'] if c in stock.columns]
        stock = stock[keep_cols].drop_duplicates(subset=['time'])

        # Tên bảng
        table_name = f"{_sanitize_symbol_for_table(symbol)}_1D"
        
        # Ghi vào PostgreSQL
        with engine.begin() as conn:
            # Tạo bảng (nếu chưa có) với PK(time)
            _ensure_table_with_pk(conn, SCHEMA, table_name)

            # Xóa sạch để replace all
            conn.execute(text(f"TRUNCATE TABLE {_quoted_ident(SCHEMA, table_name)};"))

            stock.to_sql(
                name=table_name,
                con=conn,
                schema = SCHEMA,
                if_exists='append',
                index=False,
                chunksize= 800,
                method='multi'
            )
    
        msg = f"✅ Đã lưu {symbol}"
        log.info(msg)
        return msg

    except Exception as e:
        msg = f"❌ Lỗi khi xử lý {symbol}: {str(e)}"
        log.error(msg)
        return msg

# Cập nhật toàn bộ danh sách mã cho một sàn
def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_stock, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.append(future.result())
    return messages


# Hàm tổng cho tất cả sàn — để DAG gọi
def save_DB_1D(symbol_list=None):
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    if symbol_list is not None:
        result = update_all_stocks(symbol_list)
    else:
        result = []
        # result += update_all_stocks(HOSE)
        # result += update_all_stocks(HNX)
        # result += update_all_stocks(UPCOM)
        # result += update_all_stocks(DERIVATIVES)
        # result += update_all_stocks(CW)
        # result += update_all_stocks(HNXBOND)
        # result += update_all_stocks(ETFHOSE)
        # result += update_all_stocks(indices)
        result += update_all_stocks(custom_list)
        # result = update_all_stocks(total_list)

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
    return result
