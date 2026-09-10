from sqlalchemy import create_engine, text
import pandas as pd
import concurrent.futures
import logging
from psycopg2.extras import execute_values
from utils.create_list.indices_map import indices_map
from db_config import POSTGRES_URL

log = logging.getLogger(__name__)

engine = create_engine(
    POSTGRES_URL,
    pool_size=10, max_overflow=20, pool_timeout=60
)

PRIORITY = ['VNINDEX', 'HNXINDEX', 'UPCOMINDEX']

symbol_exchange = {}
for exchange in PRIORITY:
    for symbol in indices_map.get(exchange, []):
        if symbol not in symbol_exchange:
            symbol_exchange[symbol] = exchange

def create_indicator_tables(symbols: list) -> None:
    """
    Tạo bảng indicator cho TẤT CẢ các mã (kể cả index).
    Bảng gồm 4 cột: symbol, time, rs, rsRank.
    rsRank để NULL, sẽ được script khác ghi sau.
    """
    def _create(symbol):
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(f'''
                    CREATE TABLE IF NOT EXISTS indicator."{symbol}_1D" (
                        symbol   text        NOT NULL,
                        time     timestamptz PRIMARY KEY,
                        rs       double precision,
                        "rsRank"  double precision
                    )
                ''')
        except Exception as e:
            log.error(f"❌ Tạo bảng indicator.\"{symbol}_1D\" lỗi: {e}")
 
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        list(ex.map(_create, symbols))
 
    log.info(f"✅ Đã tạo/kiểm tra {len(symbols)} bảng indicator.")
 

def calc_rs(symbol: str) -> str:
    try:
        exchange = symbol_exchange.get(symbol)
        if not exchange:
            msg = f"⚠️ Không tìm thấy exchange cho {symbol}"
            log.warning(msg)
            return msg
        # Lấy calendar từ INDEX làm timeline chuẩn 
        stock = pd.read_sql(f''' 
                SELECT time, close 
                FROM ohlcv."{symbol}_1D" 
                ORDER BY time ASC ''', engine)

        benchmark = pd.read_sql(f''' 
                SELECT time, close 
                FROM ohlcv."{exchange}_1D" 
                ORDER BY time ASC ''', engine)

        if stock.empty or benchmark.empty: 
            msg = f"⚠️ Không đủ dữ liệu cho {symbol}" 
            log.warning(msg) 
            return msg   

        stock["time"] = pd.to_datetime(stock["time"], utc=True) 
        benchmark["time"] = pd.to_datetime(benchmark["time"], utc=True) 

        stock = stock.set_index("time")["close"] 
        benchmark = benchmark.set_index("time")["close"]     

        # Timeline chuẩn = các ngày index có giao dịch 
        calendar = benchmark.index 
        # Reindex stock theo calendar index. 
        # Nếu hôm nay stock không giao dịch -> lấy close gần nhất trước đó. 
        stock = stock.reindex(calendar).ffill()

        df = pd.DataFrame({ 
            "close": stock, 
            "e_close": benchmark.reindex(calendar) 
        })

        # Không dùng dữ liệu trước ngày cổ phiếu có giá đầu tiên 
        df = df.dropna(subset=["close", "e_close"])

        for n, label in [(20, "1m"), (60, "3m"), (120, "6m")]: 
            stock_return = df["close"] / df["close"].shift(n) - 1 
            index_return = df["e_close"] / df["e_close"].shift(n) - 1 
            df[f"rs_{label}"] = ( stock_return - index_return ) * 100

        df["rs"] = df["rs_1m"] * 0.5 + df["rs_3m"] * 0.3 + df["rs_6m"] * 0.2
        df = df[["rs"]].dropna(subset=["rs"]).reset_index()

        if df.empty:
            msg = f"⚠️ Chưa đủ dữ liệu để tính rs cho {symbol}"
            log.warning(msg)
            return msg

        rows = [(symbol, row.time, row.rs) for row in df.itertuples()]
        with engine.begin() as conn:
            with conn.connection.cursor() as cur:
                execute_values(
                    cur,
                    f'''
                        INSERT INTO indicator."{symbol}_1D" (symbol, time, rs)
                        VALUES %s
                        ON CONFLICT (time) DO UPDATE
                            SET rs = EXCLUDED.rs
                    ''',
                    rows,
                    template="(%s::text, %s::timestamptz, %s::double precision)",
                    page_size=1000
                )

        msg = f"✅ Đã lưu {symbol}"
        log.info(msg)
        return msg

    except Exception as e:
        msg = f"❌ Lỗi khi xử lý {symbol}: {str(e)}"
        log.error(msg)
        return msg


def update_all_rs(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(calc_rs, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.append(future.result())
    return messages


def etl_rs():
    print("🚀 Bắt đầu tính rs...")

    # Lấy TẤT CẢ bảng _1D trong ohlcv (không lọc độ dài)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT REPLACE(table_name, '_1D', '') AS symbol
            FROM information_schema.tables
            WHERE table_schema = 'ohlcv'
            AND table_name LIKE '%\_1D'
            AND LENGTH(REPLACE(table_name, '_1D', '')) = 3
        """)).fetchall()
    all_symbols = [r[0] for r in rows]
 
    # Bước 1: Tạo bảng indicator cho TẤT CẢ (kể cả VNINDEX, HNXINDEX...)
    create_indicator_tables(all_symbols)
 
    # Bước 2: Tính rs cho các mã CỔ PHIẾU (có exchange mapping)
    stock_symbols = [s for s in all_symbols if s in symbol_exchange]
    result = update_all_rs(stock_symbols)

    errors = [msg for msg in result if msg.startswith("❌") or msg.startswith("⚠️")]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    log.info("🎉 Hoàn thành ETL rs.")
    return errors if errors else ["✅ Tất cả mã đã được xử lý thành công!"]