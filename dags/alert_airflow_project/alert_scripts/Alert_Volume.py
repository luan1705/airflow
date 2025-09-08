import logging
from datetime import datetime
from utils import connect_kafka_poll, ensure_schema_and_tables, bulk_upsert_market_data, insert_alert, load_recent_market_df

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
TOPICS = ["stock_ticks"]

def run():
    ensure_schema_and_tables()
    msgs = connect_kafka_poll(TOPICS, timeout_ms=5000, group_id="alert-Volume")
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
        df = load_recent_market_df(symbol, lookback=50)
        if len(df) < 20: continue
        avg_vol = df["volume"].iloc[-20:].mean()
        last_vol = df["volume"].iloc[-1]
        if last_vol > 2 * avg_vol:
            insert_alert("Volume", symbol, df["time"].iloc[-1], f"Volume đột biến gấp đôi trung bình (last={last_vol}, avg={avg_vol:.0f})")
            logging.info("ALERT VOLUME %s last=%s avg=%s", symbol, last_vol, avg_vol)

if __name__ == "__main__":
    run()
