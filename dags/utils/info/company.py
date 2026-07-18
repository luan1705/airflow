import requests
import pandas as pd
from sqlalchemy import create_engine, text
import concurrent.futures
import logging

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(DB_URL)

HEADERS = {
    "content-type": "application/json",
    "referer": "https://trading.vietcap.com.vn/?filter-group=HOSE&filter-value=HOSE&view-type=FLAT&type=stock",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}


def fetch_profile(symbol: str):
    try:
        res = requests.get(
            f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}",
            headers=HEADERS, timeout=30
        )
        res.raise_for_status()
        data = res.json().get("data")
        if not data:
            log.warning(f"⚠️ {symbol}: No data")
            return None
        return {
            "symbol": symbol,
            "name": data.get("viOrganName"),
            "sectorGroup": data.get("sector"),
        }
    except Exception as e:
        log.error(f"❌ {symbol}: {e}")
        return None


def info_name_sectorgroup():
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM') AND type = 'Stock'"),
        engine
    )['symbol'].tolist()

    print(f"🚀 Bắt đầu xử lý {len(symbols)} symbol...")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_profile, s): s for s in symbols}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    if not results:
        print("⚠️ Không có dữ liệu hợp lệ.")
        return

    df = pd.DataFrame(results)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO info.asset (symbol, name, "sectorGroup", type)
            VALUES (:symbol, :name, :sectorGroup, 'Stock')
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                "sectorGroup" = EXCLUDED."sectorGroup",
                type = 'Stock'
        """), df.to_dict(orient="records"))

    print(f"✅ Hoàn tất! Đã update {len(results)} symbol.")


if __name__ == "__main__":
    info_name_sectorgroup()