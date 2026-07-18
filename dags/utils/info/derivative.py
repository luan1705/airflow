import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
import logging
from utils.create_list.symbol_list import DERIVATIVES

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(DB_URL)

HEADERS = {
    "content-type": "application/json",
    "referer": "https://trading.vietcap.com.vn/?filter-group=HOSE&filter-value=HOSE&view-type=FLAT&type=stock",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}

URL_LIST  = "https://trading.vietcap.com.vn/api/price/symbols/getList"
URL_PRICE = "https://trading.vietcap.com.vn/api/price/v1/w/priceboard/ticker/price"

SYMBOLS_FUTURE     = [s for s in DERIVATIVES if s.startswith("41I")]
SYMBOLS_DERIVATIVE = [s for s in DERIVATIVES if not s.startswith("41I")]


def request_with_retry(method, url, **kwargs):
    for i in range(3):
        try:
            r = requests.request(method, url, timeout=10, **kwargs)
            if r.status_code == 200:
                return r.json()
            log.warning(f"⚠️ Retry {i+1}: {url} | status {r.status_code}")
        except Exception as e:
            log.warning(f"⚠️ Exception retry {i+1}: {e}")
        time.sleep(1)
    return None


def classify_type(symbol):
    return "Bond" if symbol.startswith("GB") else "Derivative"


def fetch_derivatives():
    data = request_with_retry("POST", URL_LIST, headers=HEADERS, json={"symbols": SYMBOLS_DERIVATIVE})
    if not isinstance(data, list):
        return []

    records = []
    for rec in data:
        listing = rec.get("listingInfo") or {}
        symbol = listing.get("symbol")
        name = listing.get("organShortName") or listing.get("organName")
        if symbol and name:
            records.append({
                "symbol":   symbol.strip().upper(),
                "name":     name.strip(),
                "exchange": "HNX",
                "type":     classify_type(symbol),
            })
    return records


def fetch_futures():
    records = []
    for symbol in SYMBOLS_FUTURE:
        data = request_with_retry("GET", f"{URL_PRICE}/{symbol}", headers=HEADERS)
        if data and isinstance(data, list):
            name = data[0].get("orgn")
            if name:
                records.append({
                    "symbol":   symbol,
                    "name":     name.strip(),
                    "exchange": "HNX",
                    "type":     "Future",
                })
    return records


def fetch_derivatives_futures():
    records = fetch_derivatives() + fetch_futures()

    if not records:
        print("⚠️ Không có dữ liệu.")
        return

    df = pd.DataFrame(records).drop_duplicates(subset=["symbol"])

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO info.asset (symbol, name, exchange, type)
            VALUES (:symbol, :name, :exchange, :type)
            ON CONFLICT (symbol) DO UPDATE SET
                name     = EXCLUDED.name,
                exchange = EXCLUDED.exchange,
                type     = EXCLUDED.type
        """), df.to_dict(orient="records"))

    print(
        f"✅ Hoàn tất! Total: {len(df)} | "
        f"Future: {(df['type']=='Future').sum()} | "
        f"Bond: {(df['type']=='Bond').sum()} | "
        f"Derivative: {(df['type']=='Derivative').sum()}"
    )


if __name__ == "__main__":
    fetch_derivatives_futures()