import requests
import pandas as pd
from sqlalchemy import create_engine, text
import re
import logging

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"


HEADERS = {
    "content-type": "application/json",
    "referer": "https://trading.vietcap.com.vn/?filter-group=HOSE&filter-value=HOSE&view-type=FLAT&type=stock",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}

URL = "https://trading.vietcap.com.vn/api/price/symbols/getList"


def clean_name(name: str) -> str:
    if not name:
        return None
    return re.sub(r"(?i)^chứng quyền\s*", "", name.strip()).strip()


def fetch_cw():
    engine = create_engine(DB_URL)
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE type = 'Warrant'"),
        engine
    )['symbol'].tolist()

    try:
        r = requests.post(URL, headers=HEADERS, json={"symbols": symbols}, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.error(f"❌ Request error: {e}")
        return

    if not data:
        log.warning("⚠️ Không có dữ liệu.")
        return

    records = []
    for rec in data:
        listing = rec.get("listingInfo", {}) or {}
        raw_name = listing.get("organShortName") or listing.get("organName")
        records.append({
            "symbol":    listing.get("symbol"),
            "name":      clean_name(raw_name),
            "organizer": listing.get("issuerName"),
        })

    df = pd.DataFrame(records).dropna(subset=["symbol"])

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO info.asset (symbol, name, organizer, type)
            VALUES (:symbol, :name, :organizer, 'Warrant')
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                organizer = EXCLUDED.organizer,
                type = 'Warrant'
        """), df.to_dict(orient="records"))

    print(f"✅ Hoàn tất! Đã upsert {len(df)} Warrant symbol.")


if __name__ == "__main__":
    fetch_cw()