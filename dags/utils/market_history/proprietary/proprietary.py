import pandas as pd
import numpy as np
import requests
import json
from pandas import json_normalize
from datetime import datetime
import logging
from sqlalchemy import create_engine

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
    data = data.rename(columns={'tradingDate': 'time'})
    intcol=['totalBuyValue','totalSellValue','totalDealBuyValue','totalDealSellValue']
    data[intcol] = (data[intcol]
                   .apply(pd.to_numeric, errors='coerce')
                   .fillna(0)
                   .astype('Int64'))
    cols=['time','totalBuyValue','totalSellValue','netMatchVal','totalBuyVolume','totalSellVolume','netMatchVol','totalDealBuyVolume','totalDealSellVolume','netDealVol','totalDealBuyValue','totalDealSellValue','netDealVal']
    data=data[cols]
    return data
  except Exception as E:
    logging.exception(f'Lỗi api propietary_{symbol}_1D')

def save_proprietary(symbol):
  try:
    mapping={
       'ALL':'ALL',
       'HSX':'HOSE',
       'HNX':'HNX',
       'UPCOM':'UPCOM'
    }
    showsymbol = mapping.get(symbol, symbol) 
    bang=f'"proprietary_{showsymbol}_1D"'
    enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
    df_db=pd.read_sql(f'SELECT "time" FROM "market_history".{bang}',enginedb)
    data=proprietary(symbol)
    df_db["time"] = pd.to_datetime(df_db["time"]).dt.date
    data["time"]  = pd.to_datetime(data["time"]).dt.date
    existing_dates = set(df_db["time"])
    new_data = data[~data["time"].isin(existing_dates)].copy()
    new_data.to_sql(name=f'proprietary_{showsymbol}_1D',
                         schema='market_history',
                         con=enginedb,
                         if_exists='append',
                         index=False
                              )
    logging.info(f'Đã lưu propietary_{symbol}_1D')
  except Exception as E:
    logging.exception(f'Lỗi lưu propietary_{symbol}_1D')

symbols=['ALL','HSX','HNX','UPCOM']
def main():
    for sym in symbols:
        save_proprietary(sym)
if __name__=='__main__':
    main()