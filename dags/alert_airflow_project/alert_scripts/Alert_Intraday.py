import logging
from datetime import datetime
from utils import connect_kafka_poll, ensure_schema_and_tables, bulk_upsert_market_data, insert_alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
TOPICS = ["stock_ticks"]

def run():
    ensure_schema_and_tables()
    msgs = connect_kafka_poll(TOPICS, timeout_ms=5000, group_id="alert-Intraday")
    records = []
    for m in msgs:
        s = m.get("symbol")
        if not s: continue
        t = m.get("time") or datetime.utcnow().isoformat()
        records.append({"symbol": s, "time": t, "open": m.get("open"), "high": m.get("high"),
                        "low": m.get("low"), "close": m.get("close"), "volume": m.get("volume")})

        # ví dụ: cảnh báo nếu volume > 1 triệu
        if m.get("volume") and m["volume"] > 1_000_000:
            insert_alert("Intraday", s, t, f"Volume đột biến {m['volume']}")
            logging.info("ALERT INTRADAY %s Volume spike %s", s, m["volume"])

    bulk_upsert_market_data(records)

if __name__ == "__main__":
    run()
