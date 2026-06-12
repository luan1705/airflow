import pandas as pd
import numpy as np
import requests
import json
from pandas import json_normalize
from datetime import datetime
import logging
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
def proprietary(symbol): 
  try:
    headers = {
      'host': 'trading.vietcap.com.vn',
      'accept': 'application/json, text/plain, */*',
      'accept-encoding': 'gzip, deflate, br, zstd',
      'accept-language': 'en-US,en;q=0.9',
      'content-type': 'application/json',
      'origin': 'https://trading.vietcap.com.vn',
      'referer': 'https://trading.vietcap.com.vn/home/',
      'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-origin',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
      }

    url = f"https://trading.vietcap.com.vn/api/fiin-api-service/v3/proprietary-trading-value?timeFrame=ONE_MONTH&market={symbol}"
    response = requests.get(url, headers=headers)
    data = response.json()['data']['data']
    data=pd.json_normalize(data)
    data['netMatchVol'] = (data['totalBuyVolume'] - data['totalSellVolume']).astype('Int64')
    data['netMatchVal'] = (data['totalBuyValue'] - data['totalSellValue']).astype('Int64')
    data['netDealVol']  = (data['totalDealBuyVolume'] - data['totalDealSellVolume']).astype('Int64')
    data['netDealVal']  = (data['totalDealBuyValue']  - data['totalDealSellValue']).astype('Int64')
    data['netVal'] = data['netMatchVal'] + data['netDealVal']
    data['netVol'] = data['netMatchVol'] + data['netDealVol']
    data = data.rename(columns={'tradingDate': 'time',
                                'totalBuyValue': 'totalBuyVal',
                                'totalSellValue': 'totalSellVal',
                                'totalBuyVolume': 'totalBuyVol',
                                'totalSellVolume': 'totalSellVol',
                                'totalDealBuyVolume': 'totalDealBuyVol',
                                'totalDealSellVolume': 'totalDealSellVol',
                                'totalDealBuyValue': 'totalDealBuyVal',
                                'totalDealSellValue': 'totalDealSellVal'
                                })
    # intcol=['totalBuyValue','totalSellValue','totalDealBuyValue','totalDealSellValue']
    # data[intcol] = (data[intcol]
    #               .apply(pd.to_numeric, errors='coerce')
    #               .fillna(0)
    #               .astype('Int64'))
    cols=['time','netVol', 'netVal', 'totalBuyVal','totalSellVal','netMatchVal','totalBuyVol','totalSellVol','netMatchVol','totalDealBuyVol','totalDealSellVol','netDealVol','totalDealBuyVal','totalDealSellVal','netDealVal']
    data=data[cols]
    return data
  except Exception as E:
    logging.exception(f'Lỗi api propietary_{symbol}_1D')

def save_proprietary(symbol, enginedb, n_last=3):
    try:
        mapping = {'HSX': 'HOSE', 'HNX': 'HNX', 'UPCOM': 'UPCOM'}
        showsymbol = mapping.get(symbol, symbol)
        table = f'"proprietary_{showsymbol}_1D"'

        data = proprietary(symbol)
        if data is None or data.empty:
            logging.warning(f'Không có dữ liệu proprietary_{symbol}_1D, bỏ qua')
            return

        data["time"] = pd.to_datetime(data["time"]).dt.date
        data = data.sort_values("time").tail(n_last)   # chỉ 3 ngày mới nhất

        cols = data.columns.tolist()
        col_list = ', '.join(f'"{c}"' for c in cols)
        update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'time')

        def to_py(v):
            return None if pd.isna(v) else (v.item() if hasattr(v, "item") else v)

        rows_data = [tuple(to_py(v) for v in r)
                     for r in data.itertuples(index=False, name=None)]

        with enginedb.begin() as conn:
            with conn.connection.cursor() as cur:
                execute_values(cur, f"""
                    INSERT INTO exchange_history.{table} ({col_list})
                    VALUES %s
                    ON CONFLICT (time) DO UPDATE SET {update_set}
                """, rows_data, page_size=1000)

        logging.info(f'Đã upsert proprietary_{symbol}_1D ({len(data)} dòng mới nhất)')
    except Exception as E:
        logging.exception(f'Lỗi lưu proprietary_{symbol}_1D')

symbols=['HSX','HNX','UPCOM']
def main():
    enginedb = create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")
    try:
        for sym in symbols:
            save_proprietary(sym, enginedb)
    finally:
        enginedb.dispose()  # đóng pool sau khi chạy xong
        logging.info("🔒 Đã đóng kết nối DB")