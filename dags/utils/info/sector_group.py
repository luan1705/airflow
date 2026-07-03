import requests
import pandas as pd
from sqlalchemy import create_engine, text
import concurrent.futures
import logging

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)

HEADERS = {
    "content-type": "application/json",
    "referer": "https://trading.vietcap.com.vn/?filter-group=HOSE&filter-value=HOSE&view-type=FLAT&type=stock",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}


def fetch_sector(symbol: str):
    try:
        res = requests.get(
            f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}",
            headers=HEADERS, timeout=30
        )
        res.raise_for_status()
        data = res.json().get("data")
        if not data:
            return None
        sector_en = data.get("sector")
        sector_vi = data.get("sectorVn")
        if not sector_en or not sector_vi:
            return None
        return {"sectorGroup": sector_en.strip(), "name": sector_vi.strip()}
    except Exception as e:
        log.error(f"❌ {symbol}: {e}")
        return None


def update_sector_group():
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM') AND type = 'Stock'"),
        engine
    )['symbol'].tolist()

    print(f"🚀 Bắt đầu xử lý {len(symbols)} symbol...")
    sector_group = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_sector, s): s for s in symbols}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                sector_group[result["sectorGroup"]] = result["name"]

    if not sector_group:
        print("⚠️ Không có dữ liệu.")
        return

    records = [{"sectorGroup": k, "name": v} for k, v in sector_group.items()]

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO info.sector_group ("sectorGroup", name)
            VALUES (:sectorGroup, :name)
            ON CONFLICT ("sectorGroup") DO UPDATE SET
                name = EXCLUDED.name
        """), records)

    print(f"✅ Hoàn tất! Đã upsert {len(records)} sector group.")


if __name__ == "__main__":
    update_sector_group()