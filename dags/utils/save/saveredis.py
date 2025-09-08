import redis
import pandas as pd
import json
import logging
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
from .List.HOSE import hose 
from .List.HNX import hnx 
from .List.UPCOM import upcom  

# Logging
log = logging.getLogger(__name__)

# Kết nối CSDL
engine = create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')

# Kết nối Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Danh sách mã
symbol_list = hose + hnx + upcom
SCHEMA = "history_tradingview"

# Hàm lấy dữ liệu từ PostgreSQL và lưu vào Redis
def get_data_and_cache(symbol):
    try:
        query = text(f"""
            SELECT "close", "time", "volume"
            FROM "{SCHEMA}"."{symbol}_1D"
            ORDER BY time DESC
            LIMIT 100
        """)
        df = pd.read_sql(query, con=engine)

        if df.empty:
            print(f"⚠️ Không có dữ liệu: {symbol}")
            return None

        df = df.sort_values('time', ascending=True).reset_index(drop=True)
        redis_list = [
            json.dumps({
                "time": row["time"],
                "close": row["close"],
                "TotalVolume": row["volume"]
            }) for _, row in df.iterrows()
        ]
        redis_key = f"{SCHEMA}: {symbol}"
        r.delete(redis_key)
        r.rpush(redis_key, *redis_list)
        print(f"✅ Đã lưu Redis: {symbol}")
        return symbol

    except Exception as e:
        print(f"Lỗi {symbol}: {e}")
        return None


# Hàm chạy đa luồng
def run_multithreaded_cache():
    messages = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(get_data_and_cache, symbol) for symbol in symbol_list]
        for future in as_completed(futures):
            result = future.result()
            messages.append(result)
    return messages

# Chạy chính
def save_rd():
    log.info(f"🚀 Bắt đầu lưu Redis cho {len(symbol_list)} mã...")

    results = run_multithreaded_cache()
    errors = [msg for msg in results if msg.startswith("❌") or msg.startswith("⚠️")]

    log.info(f"✅ Tổng số mã xử lý: {len(results)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if len(errors) > 0:
        raise Exception("⚠️ Một số mã không được lưu vào Redis:\n" + "\n".join(errors))

    log.info("🎉 Hoàn tất lưu Redis.")
    return results
