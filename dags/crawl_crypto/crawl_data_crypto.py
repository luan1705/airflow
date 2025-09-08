import requests
import pandas as pd
import psycopg2
from psycopg2 import extras  
import websocket
import json
import threading
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== Config ==================
DB_CONFIG = {
    "host": "videv.cloud",
    "port": 5432,
    "database": "vnsfintech",
    "user": "vnsfintech",
    "password": "@Vns123456"
}
TOP_LIMIT = 100   # số lượng crypto top
INTERVAL = "1m"   # khung thời gian lịch sử

# ================== DB Setup ==================
def init_db():
    # Không tạo schema nữa, chỉ kiểm tra kết nối
    conn = psycopg2.connect(**DB_CONFIG)
    conn.close()

def save_to_db(symbol, df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    table = f"cryptocurrency.{symbol.lower()}"

    # 👉 Lấy thời gian mới nhất trong DB
    cur.execute(f"SELECT MAX(time) FROM {table};")
    last_time = cur.fetchone()[0]

    if last_time:
        # lọc ra chỉ giữ data mới hơn
        df = df[df["time"] > last_time]

    if df.empty:
        print(f"⏩ {symbol}: không có dữ liệu mới, skip.")
        cur.close()
        conn.close()
        return

    insert_query = f"""
        INSERT INTO {table}(time, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (time) DO NOTHING;
    """

    data = [
        (row["time"], row["open"], row["high"], row["low"], row["close"], row["volume"])
        for _, row in df.iterrows()
    ]

    extras.execute_batch(cur, insert_query, data, page_size=100)
    conn.commit()

    print(f"✅ {symbol}: thêm {len(df)} dòng mới")

    cur.close()
    conn.close()

# ================== Binance API ==================
def time_to_str(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def get_top_symbols(limit=100):
    url = "https://api.binance.com/api/v3/ticker/24hr"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    data = sorted(data, key=lambda x: float(x["quoteVolume"]), reverse=True)
    symbols = [item["symbol"] for item in data if item["symbol"].endswith("USDT")]
    return symbols[:limit]

def crypto_history(symbol, interval="1m", limit=100, retries=3):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return pd.DataFrame([{
                "time":   datetime.fromtimestamp(item[0] / 1000),
                "open":   float(item[1]),
                "high":   float(item[2]),
                "low":    float(item[3]),
                "close":  float(item[4]),
                "volume": float(item[5])
            } for item in data])
        except Exception as e:
            print(f"⚠️ {symbol} lỗi lần {attempt+1}/{retries}: {e}")
            time.sleep(2)
    return pd.DataFrame()

# ================== WebSocket Streaming ==================
def on_message(ws, message):
    data = json.loads(message)
    print("Stream:", data)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)

def start_stream(symbols):
    params = [f"{s.lower()}@trade" for s in symbols]  # trade stream
    stream_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(params)}"

    ws = websocket.WebSocketApp(
        stream_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    wst = threading.Thread(target=ws.run_forever)
    wst.start()

# ================== Main ==================
if __name__ == "__main__":
    print("🔹 Kết nối DB...")
    init_db()

    print("🔹 Lấy top 100 crypto...")
    top_symbols = get_top_symbols(limit=TOP_LIMIT)
    print("Top 100:", top_symbols)

    print("🔹 Lưu lịch sử giá vào DB bằng threading...")

    def process_symbol(sym):
        df = crypto_history(sym, interval=INTERVAL)
        if not df.empty:
            save_to_db(sym, df)
            return f"{sym} lưu OK"
        else:
            return f"{sym} bỏ qua (fail)"

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {executor.submit(process_symbol, sym): sym for sym in top_symbols}

        for i, future in enumerate(as_completed(future_to_symbol), 1):
            sym = future_to_symbol[future]
            try:
                result = future.result()
                print(f"{i}/{len(top_symbols)} {result}")
            except Exception as e:
                print(f"{i}/{len(top_symbols)} {sym} lỗi: {e}")
