import requests
import json
import pandas as pd
from sqlalchemy import create_engine,text
from psycopg2.extras import execute_values
import concurrent.futures
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE, indices, addition
import logging
from datetime import datetime, timedelta
import numpy as np
from pandas import json_normalize

# Thiết lập logging 
log=logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
)
create_table_sql = """
    CREATE TABLE IF NOT EXISTS dividend.dividend (
        "symbol"  TEXT NOT NULL,
        "date"    DATE NOT NULL,
        "payType" TEXT NOT NULL,
        "cash"    DOUBLE PRECISION,
        "stock"   DOUBLE PRECISION,
        PRIMARY KEY ("symbol", "date", "payType")
    );
"""

def dividend(symbol):
    try:
        headers = {
            'content-type': 'application/json',
            'referer': 'https://trading.vietcap.com.vn/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }

        now=datetime.today()
        from_date = (now - timedelta(days=400)).strftime('%Y%m%d')
        to_date = now.strftime('%Y%m%d')
        params = {
            'ticker': symbol.upper(),
            "fromDate": from_date,
            "toDate": to_date,
            "eventCode":["DIV","ISS"],
            "size": 50000
        }
        url = 'https://iq.vietcap.com.vn/api/iq-insight-service/v1/events'
        response = requests.get(url, headers=headers, params=params)
        data = json_normalize(response.json()['data']['content'])
        cols=['ticker','eventNameVi', 'eventTitleVi','exerciseRatio', 'exrightDate']
        date_cols=[ 'exrightDate']
        data=data[cols]
        for col in date_cols:
            data[col] = pd.to_datetime(data[col], errors='coerce').dt.floor('d')
        data['eventTitleVi']=data['eventTitleVi'].replace(r'^.*?-\s*','',regex=True)
        rename_dict = {
            'ticker': 'symbol',
            'eventNameVi': 'eventName',
            'eventTitleVi': 'eventTitle',
            'exerciseRatio': 'ratio',
            'exrightDate': 'date'
        }
        data=data.rename(columns=rename_dict)
        cash_mask  = data['eventName'] == 'Trả cổ tức bằng tiền mặt'
        stock_mask = data['eventTitle'].str.contains('Trả Cổ tức bằng Cổ phiếu', na=False)
        data = data[cash_mask | stock_mask].copy()
        data = data.dropna(subset=['date'])

        cash_rows = data['eventName'] == 'Trả cổ tức bằng tiền mặt'
        data['payType'] = np.where(cash_rows, 'cash', 'stock')

        # cash: số tiền VND lấy từ eventTitle, làm tròn int
        data['cash'] = pd.NA
        data.loc[cash_rows, 'cash'] = (
            data.loc[cash_rows, 'eventTitle']
            .str.extract(r'([\d,\.]+)\s*VND', expand=False)
            .str.replace(',', '', regex=False)
            .astype(float)
            .round(0)
            .values
        )
        data['cash'] = data['cash'].astype('Float64')

        # stock: tỉ lệ từ exerciseRatio
        data['stock'] = data['ratio'].where(~cash_rows).astype('Float64')
        data = data[['symbol', 'date', 'payType', 'cash', 'stock']]
        data = data.sort_values(['date', 'payType']).reset_index(drop=True)
        data['date'] = data['date'].dt.date 
        return data
    except Exception as e :
            print(f"Lỗi: {e}")
            return pd.DataFrame()

def save_pg(symbol):
    try:
        symbol = symbol.upper()

        df = dividend(symbol)
        if df.empty:
            return f"⚠ Không có dữ liệu dividend {symbol}"

        records = (
            df.astype(object)
              .where(df.notna(), None)
              .to_records(index=False)
              .tolist()
        )

        upsert_sql = """
            INSERT INTO dividend.dividend ("symbol", "date", "payType", "cash", "stock")
            VALUES %s
            ON CONFLICT ("symbol", "date", "payType") DO UPDATE SET
                "cash"  = EXCLUDED."cash",
                "stock" = EXCLUDED."stock";
        """

        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                execute_values(cur, upsert_sql, records, page_size=500)
            raw_conn.commit()
        finally:
            raw_conn.close()

        return f"✔ {symbol}: {len(df)} rows upserted"
    except Exception as e:
        return f"❌ Lỗi {symbol}: {e}"

def update_all_symbol(symbol_list):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_pg, sym): sym for sym in symbol_list}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return results

def save_all_pg():
    with engine.begin() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS dividend;'))
        conn.execute(text(create_table_sql))
    result = []
    result += update_all_symbol(HOSE)
    result += update_all_symbol(HNX)
    result += update_all_symbol(UPCOM)
    # result += update_all_symbol(DERIVATIVES)
    # result += update_all_symbol(CW)
    # result += update_all_symbol(HNXBOND)
    # result += update_all_symbol(ETFHOSE)
    # result += update_all_symbol(indices)
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
    return result
