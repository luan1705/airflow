import logging
import pandas as pd
from datetime import datetime
from utils import connect_kafka_poll, ensure_schema_and_tables, bulk_upsert_market_data, insert_alert, load_recent_market_df

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
TOPICS = ["stock_ticks"]   # chỉnh theo Kafka thực tế

def compute_macd(df):
    df = df.copy()
    df["EMA12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["EMA26"] = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = df["EMA12"] - df["EMA26"]
    df["Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    return df

def run():
    ensure_schema_and_tables()
    msgs = connect_kafka_poll(TOPICS, timeout_ms=5000, group_id="alert-MACD")
    records, grouped = [], {}
    for m in msgs:
        s = m.get("symbol")
        if not s: continue
        t = m.get("time") or datetime.utcnow().isoformat()
        records.append({"symbol": s, "time": t, "open": m.get("open"), "high": m.get("high"),
                        "low": m.get("low"), "close": m.get("close"), "volume": m.get("volume")})
        grouped.setdefault(s, []).append(m)
    bulk_upsert_market_data(records)

    for symbol, msgs_for_sym in grouped.items():
        hist = load_recent_market_df(symbol, lookback=200)
        if hist.empty: continue
        df = compute_macd(hist)
        if len(df) < 2: continue
        if df["MACD"].iloc[-2] < df["Signal"].iloc[-2] and df["MACD"].iloc[-1] > df["Signal"].iloc[-1]:
            t = df["time"].iloc[-1]
            insert_alert("MACD", symbol, t, "MACD cắt lên Signal → Bullish")
            logging.info("ALERT MACD UP %s", symbol)
        elif df["MACD"].iloc[-2] > df["Signal"].iloc[-2] and df["MACD"].iloc[-1] < df["Signal"].iloc[-1]:
            t = df["time"].iloc[-1]
            insert_alert("MACD", symbol, t, "MACD cắt xuống Signal → Bearish")
            logging.info("ALERT MACD DOWN %s", symbol)

if __name__ == "__main__":
    run()
