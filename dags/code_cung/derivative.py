import requests
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Text
import time

# ===================== CONFIG ===================== #
DB_URL = "postgresql://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)

STOCK_SCHEMA = "info"
STOCK_TABLE = "asset"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://trading.vietcap.com.vn",
    "referer": "https://trading.vietcap.com.vn/price-board",
    "user-agent": "Mozilla/5.0",
}

URL = "https://trading.vietcap.com.vn/api/price/symbols/getList"

# ===================== SYMBOL LIST ===================== #
symbols_hdtl_tpcp = [
    '41I1G1000', '41I1G3000', '41I1G2000', '41I1G6000',
    '41I2G1000', '41I2G2000', '41I2G3000', '41I2G6000',
    '41B5G3000', '41B5G6000', '41B5G9000',
    '41BAG3000', '41BAG6000', '41BAG9000','GB10F2512'
]

all_symbols = symbols_hdtl_tpcp

# ===================== DB TABLE ===================== #
metadata = MetaData(schema=STOCK_SCHEMA)

stock_table = Table(
    STOCK_TABLE, metadata,
    Column("symbol", Text, primary_key=True, quote=True),
    Column("name", Text, quote=True),
    Column("exchange", Text, quote=True),
    Column("type", Text, quote=True),
)

# ===================== FUNCTIONS ===================== #
def get_symbols_info(symbols: list):
    payload = {"symbols": symbols}
    try:
        r = requests.post(URL, headers=HEADERS, json=payload, timeout=10)
        print("Status:", r.status_code)
        return r.json()
    except Exception as e:
        print("Request error:", e)
        return None


def transform_record(record: dict):
    listing = record.get("listingInfo", {}) or {}

    return {
        "symbol": listing.get("symbol"),
        "name": listing.get("organShortName") or listing.get("organName"),
        "exchange": listing.get("board"),
    }


def upsert_stock(df: pd.DataFrame):
    with engine.begin() as conn:
        for _, row in df.iterrows():
            if not row["symbol"]:
                continue

            conn.execute(
                text(f"""
                    INSERT INTO {STOCK_SCHEMA}.{STOCK_TABLE}
                        (symbol, name, exchange, type)
                    VALUES
                        (:symbol, :name, :exchange, 'Derivative')
                    ON CONFLICT (symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        type = 'Derivative';
                """),
                {
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "exchange": row["exchange"],
                }
            )

# ===================== MAIN ===================== #
if __name__ == "__main__":
    while True:
        data = get_symbols_info(all_symbols)
        if data:
            records = [transform_record(rec) for rec in data]
            df = pd.DataFrame(records)

            upsert_stock(df)
            print(f"Upsert {len(df)} symbols into info.stock (Derivative) ✅")

        print("=== sleep 1s ===")
        time.sleep(1)
