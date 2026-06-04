import requests
import json
import pandas as pd
from sqlalchemy import create_engine,text
from psycopg2.extras import execute_values
import concurrent.futures
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, addition
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
)

def free_float(symbol):
    symbol = symbol.upper()

    url = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/details?ticker={symbol}"

    headers = {
        "Referer": "https://trading.vietcap.com.vn/",
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(url, headers=headers).json()['data']

        if not response:
            raise ValueError("Empty response")

        df = pd.json_normalize(response)[
            ['ticker', 'freeFloatPercentage']
        ]

        df.columns = ['symbol', 'freeFloatPct']

        return df

    except Exception as e:
        print(f"❌ {symbol}: {e}")

        return pd.DataFrame({
            'symbol': [symbol],
            'freeFloatPct': [None]
        })

def save_pg(symbol):
    df = free_float(symbol)

    if df['freeFloatPct'].isna().all():
        return f"❌ {symbol}: no data"
    
    df['freeFloatPct'] = df['freeFloatPct'].round(4)

    with engine.begin() as conn:
        conn.execute(
            text('UPDATE info.asset SET "freeFloatPct" = :val WHERE symbol = :sym'),
            {"sym": symbol.upper(), "val": df['freeFloatPct'].iloc[0]}
        )
    log.info(f"✅ Đã lưu {symbol}") 
    return f"✔ {symbol}: updated"

def update_all_symbol(symbol_list):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_pg, sym): sym for sym in symbol_list}
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                results.append(f"❌ {futures[future]}: {e}")  # ← bắt lỗi DB
    return results

def save_all_pg():
    result = []
    result += update_all_symbol(HOSE)
    result += update_all_symbol(HNX)
    result += update_all_symbol(UPCOM)

    # result += update_all_symbol(addition)
    
    errors = [msg for msg in result if msg.startswith("❌") or msg.startswith("⚠️")]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    # if len(errors) >= 5:
    #     raise Exception("Task thất bại vì có lỗi:\n" + "\n".join(errors))

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return errors