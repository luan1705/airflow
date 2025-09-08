import logging
import pandas as pd
from datetime import datetime
from utils import connect_kafka_poll, ensure_schema_and_tables, bulk_upsert_market_data, insert_alert, load_recent_market_df

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
TOPICS = ["stock_ticks"]

def compute_cross(df):
    df = df.copy()
    df["MA50"] = df["close"].rolling(window=50).mean()
    df["MA200"] = df["close"].rolling(window=200).mean()
    return df

def run():
    ensure_schema_and_tables()
    msgs = connect_kafka_poll(TOPICS, timeout_ms=5000, group_id="alert-MAcross")
    records, grouped = [], {}
    for m in msgs:
        s = m.get("symbol")
        if not s: continue
        t = m.get("time") or datetime.utcnow().isoformat()
        records.append({"symbol": s, "time": t, "open": m.get("open"), "high": m.get("high"),
                        "low": m.get("low"), "close": m.get("close"), "volume": m.get("volume")})
        grouped.setdefault(s, []).append(m)
    bulk_upsert_market_data(records)

    for symbol in grouped:
        df = load_recent_market_df(symbol, lookback=300)
        if len(df) < 200: continue
        df = compute_cross(df)
        if len(df) < 2: continue
        if df["MA50"].iloc[-2] < df["MA200"].iloc[-2] and df["MA50"].iloc[-1] > df["MA200"].iloc[-1]:
            insert_alert("MAcross", symbol, df["time"].iloc[-1], "Golden Cross (MA50 cắt lên MA200)")
            logging.info("ALERT Golden Cross %s", symbol)
        elif df["MA50"].iloc[-2] > df["MA200"].iloc[-2] and df["MA50"].iloc[-1] < df["MA200"].iloc[-1]:
            insert_alert("MAcross", symbol, df["time"].iloc[-1], "Death Cross (MA50 cắt xuống MA200)")
            logging.info("ALERT Death Cross %s", symbol)

if __name__ == "__main__":
    run()
