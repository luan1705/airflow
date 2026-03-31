from zoneinfo import ZoneInfo

import redis
import pandas as pd
import json
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.create_list.symbol_list import total_list

# Kết nối DB
engine = create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")

# Kết nối Redis
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=2.5,           # timeout đọc/ghi
    socket_connect_timeout=2.0,   # timeout connect
    health_check_interval=30,     # ping định kỳ 30s
    max_connections=30,            # Mỗi container chỉ tối đa 3 socket tới Redis
    timeout=1.0,                  # Khi pool bận, chờ tối đa 1s để lấy connection (không drop)
)
r = redis.Redis(connection_pool=POOL)

# Danh sách mã
symbol_list = total_list
SCHEMA = "ohlcv"

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def _to_vn_time(ts):
    """
    ts có thể:
    - timezone-aware (có tzinfo) -> tz_convert sang VN
    - naive (không tzinfo) -> giả sử là UTC rồi convert sang VN
    """
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(VN_TZ)

# Hàm lấy dữ liệu từ PostgreSQL và lưu vào Redis
def get_data_and_cache(symbol):
    query = text(f"""
        SELECT o."time", o."symbol", o."open", o."high", o."low", o."close", o."volume",a.exchange,a.indices
        FROM "{SCHEMA}"."{symbol}_1D" o left join info.asset a on o.symbol = a.symbol
        WHERE o."time"::date != CURRENT_DATE
        ORDER BY o."time" DESC
        LIMIT 200
    """)
    try:
        df = pd.read_sql(query, con=engine)
        if not df.empty:
            df = df.sort_values('time', ascending=True).reset_index(drop=True)
            df["time_vn"] = df["time"].apply(_to_vn_time)
            redis_list = [
                json.dumps({
                    "time": row["time_vn"].date().isoformat(),
                    "symbol": row["symbol"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "exchange": row["exchange"],
                    "indices": row["indices"]
                }) for _, row in df.iterrows()
            ]
            redis_key = f"{SCHEMA}:{symbol}"
            r.delete(redis_key)
            r.rpush(redis_key, *redis_list)
            print(f"✅ Đã lưu Redis: {symbol}")
            return symbol
        else:
            print(f"⚠️ Không có dữ liệu: {symbol}")
    except Exception as e:
        print(f"Lỗi {symbol}:{e}")
    return None

# Hàm chạy đa luồng
def run_multithreaded_cache():
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(get_data_and_cache, symbol) for symbol in symbol_list]

        for future in as_completed(futures):
            _ = future.result()  # Có thể xử lý kết quả nếu cần

# Chạy chính
if __name__ == "__main__":
    print(f"🚀 Bắt đầu lưu dữ liệu Redis cho {len(symbol_list)} mã...")
    run_multithreaded_cache()
    print("✅ Hoàn tất lưu Redis.")

