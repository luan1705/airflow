import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
import re

# ===================== CONFIG ===================== #
DB_URL = "postgresql://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
engine = create_engine(DB_URL)

STOCK_SCHEMA = "info"
STOCK_TABLE = "asset"

HEADERS = {
    "host": "trading.vietcap.com.vn",
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://trading.vietcap.com.vn",
    "referer": "https://trading.vietcap.com.vn/price-board",
    "user-agent": "Mozilla/5.0",
    "device-id": "198a946196213743"
}

URL = "https://trading.vietcap.com.vn/api/price/symbols/getList"

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


def clean_name(name: str) -> str:
    if not name:
        return None
    name = re.sub(r"(?i)^chứng quyền\s*", "", name.strip())
    return name.strip()


def transform_record(record: dict):
    listing = record.get("listingInfo", {}) or {}

    raw_name = listing.get("organShortName") or listing.get("organName")

    return {
        "symbol": listing.get("symbol"),
        "name": clean_name(raw_name),
        "organizer": listing.get("issuerName"),
    }


def upsert_stock(df: pd.DataFrame):
    with engine.begin() as conn:
        for _, row in df.iterrows():
            if not row["symbol"]:
                continue

            conn.execute(
                text(f"""
                    INSERT INTO {STOCK_SCHEMA}.{STOCK_TABLE}
                        (symbol, name, organizer, type)
                    VALUES
                        (:symbol, :name, :organizer, 'Warrant')
                    ON CONFLICT (symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        organizer = EXCLUDED.organizer,
                        type = 'Warrant';
                """),
                {
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "organizer": row["organizer"],
                }
            )

# ===================== MAIN ===================== #
if __name__ == "__main__":

    symbols =  [
    "CACB2502",	"CACB2508",	"CACB2509",	"CACB2510",	"CACB2511",	"CACB2512",	"CACB2513",	"CACB2514",	"CACB2515",	"CACB2516",
    "CACB2517",	"CFPT2503",	"CFPT2505",	"CFPT2508",	"CFPT2509",	"CFPT2510",	"CFPT2511",	"CFPT2512",	"CFPT2513",	"CFPT2515",
    "CFPT2516",	"CFPT2517",	"CFPT2518",	"CFPT2519",	"CFPT2520",	"CFPT2521",	"CFPT2522",	"CFPT2523",	"CFPT2524",	"CFPT2525",
    "CFPT2526",	"CFPT2527",	"CFPT2528",	"CFPT2529",	"CFPT2530",	"CFPT2531",	"CFPT2532",	"CFPT2533",	"CHDB2504",	"CHDB2505",
    "CHDB2506",	"CHDB2507",	"CHDB2508",	"CHDB2509",	"CHPG2505",	"CHPG2506",	"CHPG2510",	"CHPG2514",	"CHPG2515",	"CHPG2516",
    "CHPG2517",	"CHPG2518",	"CHPG2520",	"CHPG2521",	"CHPG2522",	"CHPG2523",	"CHPG2524",	"CHPG2525",	"CHPG2526",	"CHPG2527",
    "CHPG2528",	"CHPG2529",	"CHPG2530",	"CHPG2531",	"CHPG2532",	"CHPG2533",	"CHPG2534",	"CHPG2535",	"CHPG2536",	"CHPG2537",
    "CHPG2538",	"CHPG2539",	"CHPG2540",	"CHPG2541",	"CLPB2501",	"CLPB2503",	"CLPB2504",	"CLPB2505",	"CLPB2506",	"CLPB2507",
    "CLPB2508",	"CLPB2509",	"CMBB2504",	"CMBB2505",	"CMBB2507",	"CMBB2509",	"CMBB2510",	"CMBB2511",	"CMBB2513",	"CMBB2514",
    "CMBB2515",	"CMBB2516",	"CMBB2517",	"CMBB2518",	"CMBB2519",	"CMBB2520",	"CMBB2521",	"CMBB2522",	"CMBB2523",	"CMSN2508",
    "CMSN2509",	"CMSN2510",	"CMSN2511",	"CMSN2512",	"CMSN2514",	"CMSN2515",	"CMSN2516",	"CMSN2517",	"CMSN2518",	"CMSN2519",
    "CMSN2520",	"CMSN2521",	"CMSN2522",	"CMWG2504",	"CMWG2507",	"CMWG2508",	"CMWG2509",	"CMWG2510",	"CMWG2511",	"CMWG2513",
    "CMWG2514",	"CMWG2515",	"CMWG2516",	"CMWG2517",	"CMWG2518",	"CMWG2519",	"CMWG2520",	"CMWG2521",	"CMWG2522",	"CMWG2523",
    "CMWG2524",	"CMWG2525",	"CMWG2526",	"CMWG2527",	"CSHB2504",	"CSHB2505",	"CSHB2506",	"CSHB2507",	"CSHB2508",	"CSHB2509",
    "CSHB2510",	"CSHB2511",	"CSHB2512",	"CSHB2513",	"CSHB2514",	"CSSB2503",	"CSSB2504",	"CSSB2505",	"CSSB2506",	"CSSB2507",
    "CSSB2508",	"CSSB2509",	"CSTB2510",	"CSTB2511",	"CSTB2512",	"CSTB2513",	"CSTB2514",	"CSTB2515",	"CSTB2517",	"CSTB2518",
    "CSTB2519",	"CSTB2520",	"CSTB2521",	"CSTB2522",	"CSTB2523",	"CSTB2524",	"CSTB2525",	"CSTB2526",	"CSTB2527",	"CSTB2528",
    "CSTB2529",	"CSTB2530",	"CSTB2531",	"CSTB2532",	"CSTB2533",	"CSTB2534",	"CSTB2535",	"CSTB2536",	"CSTB2537",	"CTCB2504",
    "CTCB2507",	"CTCB2509",	"CTCB2510",	"CTCB2511",	"CTCB2512",	"CTCB2513",	"CTCB2514",	"CTCB2515",	"CTCB2516",	"CTCB2517",
    "CTCB2518",	"CTCB2519",	"CTCB2520",	"CTCB2521",	"CTCB2522",	"CTCB2523",	"CTPB2502",	"CTPB2503",	"CTPB2504",	"CTPB2505",
    "CTPB2506",	"CTPB2507",	"CTPB2508",	"CTPB2509",	"CTPB2510",	"CVHM2503",	"CVHM2508",	"CVHM2509",	"CVHM2510",	"CVHM2511",
    "CVHM2512",	"CVHM2514",	"CVHM2515",	"CVHM2516",	"CVHM2517",	"CVHM2518",	"CVHM2519",	"CVHM2520",	"CVHM2521",	"CVHM2522",
    "CVHM2523",	"CVHM2524",	"CVIB2504",	"CVIB2505",	"CVIB2507",	"CVIB2508",	"CVIB2509",	"CVIB2510",	"CVIB2511",	"CVIB2512",
    "CVIB2513",	"CVIC2507",	"CVIC2508",	"CVIC2509",	"CVIC2510",	"CVIC2511",	"CVIC2513",	"CVIC2514",	"CVIC2515",	"CVIC2516",
    "CVJC2504",	"CVJC2505",	"CVJC2506",	"CVNM2503",	"CVNM2508",	"CVNM2509",	"CVNM2510",	"CVNM2511",	"CVNM2513",	"CVNM2514",
    "CVNM2515",	"CVNM2516",	"CVNM2517",	"CVNM2518",	"CVNM2519",	"CVNM2520",	"CVNM2521",	"CVNM2522",	"CVNM2523",	"CVPB2502",
    "CVPB2504",	"CVPB2509",	"CVPB2510",	"CVPB2511",	"CVPB2512",	"CVPB2513",	"CVPB2515",	"CVPB2516",	"CVPB2517",	"CVPB2518",
    "CVPB2519",	"CVPB2520",	"CVPB2521",	"CVPB2522",	"CVPB2523",	"CVPB2524",	"CVPB2525",	"CVPB2526",	"CVPB2527",	"CVPB2528",
    "CVPB2529",	"CVPB2530",	"CVPB2531",	"CVPB2532",	"CVRE2509",	"CVRE2510",	"CVRE2511",	"CVRE2512",	"CVRE2513",	"CVRE2515",
    "CVRE2516",	"CVRE2517",	"CVRE2518",	"CVRE2519",	"CVRE2520",	"CVRE2521",	"CVRE2522",	"CVRE2523",	"CVRE2524",	"CVRE2525",
    "CVRE2526",
]  # demo, giữ list dài của bạn nếu muốn

    while True:
        data = get_symbols_info(symbols)
        if data:
            records = [transform_record(rec) for rec in data]
            df = pd.DataFrame(records)

            upsert_stock(df)
            print(f"Upsert {len(df)} Warrant symbols into info.stock ✅")

        print("=== sleep 1s ===")
        time.sleep(1)
