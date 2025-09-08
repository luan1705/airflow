import logging
import pandas as pd
from datetime import datetime
from utils import connect_kafka_poll, ensure_schema_and_tables, bulk_upsert_market_data, insert_alert, load_recent_market_df

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
TOPICS = ["stock_ticks"]   # chỉnh theo Kafka thực tế

def compute_ma_signals(df):
    df = df.copy()
    df['MA20'] = df['close'].rolling(window=20).mean()
    return df

def run():
    ensure_schema_and_tables()
    msgs = connect_kafka_poll(TOPICS, timeout_ms=5000, group_id="alert-MA")
    records = []
    grouped = {}
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
        new_rows = [{"time": m.get("time") or datetime.utcnow().isoformat(), "close": m.get("close")} for m in msgs_for_sym]
        if not new_rows and hist.empty:
            continue
        df_new = pd.DataFrame(new_rows)
        if not df_new.empty:
            df_new['close'] = df_new['close'].astype(float)
            df = pd.concat([hist[['time','close']], df_new], ignore_index=True)
        else:
            df = hist[['time','close']].copy()
        df = df.sort_values('time').reset_index(drop=True)
        if len(df) < 2: continue
        df = compute_ma_signals(df)
        if len(df) < 2: continue
        if df['close'].iloc[-2] < df['MA20'].iloc[-2] and df['close'].iloc[-1] > df['MA20'].iloc[-1]:
            t = df['time'].iloc[-1]
            message = f"close {df['close'].iloc[-1]} cắt lên MA20 {df['MA20'].iloc[-1]:.2f}"
            insert_alert("MA", symbol, t, message)
            logging.info("ALERT MA UP %s %s", symbol, message)
        elif df['close'].iloc[-2] > df['MA20'].iloc[-2] and df['close'].iloc[-1] < df['MA20'].iloc[-1]:
            t = df['time'].iloc[-1]
            message = f"close {df['close'].iloc[-1]} cắt xuống MA20 {df['MA20'].iloc[-1]:.2f}"
            insert_alert("MA", symbol, t, message)
            logging.info("ALERT MA DOWN %s %s", symbol, message)

if __name__ == "__main__":
    run()
