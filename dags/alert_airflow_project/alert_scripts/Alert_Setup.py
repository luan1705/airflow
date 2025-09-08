import logging
from datetime import datetime
from utils import connect_kafka_poll, ensure_schema_and_tables, bulk_upsert_market_data, insert_alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
TOPICS = ["stock_ticks"]

def run():
    ensure_schema_and_tables()
    msgs = connect_kafka_poll(TOPICS, timeout_ms=5000, group_id="alert-Setup")
    records = []
    for m in msgs:
        s = m.get("symbol")
        if not s: continue
        t = m.get("time") or datetime.utcnow().isoformat()
        records.append({"symbol": s, "time": t, "open": m.get("open"), "high": m.get("high"),
                        "low": m.get("low"), "close": m.get("close"), "volume": m.get("volume")})

        # ví dụ setup: giá tăng >5% trong ngày
        if m.get("pct_change") and m["pct_change"] > 5:
            insert_alert("Setup", s, t, f"Tăng mạnh {m['pct_change']}%")
            logging.info("ALERT SETUP %s tăng mạnh %s%%", s, m["pct_change"])

    bulk_upsert_market_data(records)

if __name__ == "__main__":
    run()
