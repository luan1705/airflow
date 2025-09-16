import pandas as pd
import numpy as np
import requests
import json
from pandas import json_normalize
from datetime import datetime,timezone,timedelta
import logging
from sqlalchemy import create_engine
import logging
logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)


def to_timestamp_utc(date_str: str, hour=0, minute=0, second=0) -> int:
    """Convert 'YYYY-MM-DD' + (hour:minute:second) UTC -> Unix timestamp (seconds)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc, hour=hour, minute=minute, second=second, microsecond=0)
    return int(dt.timestamp())

def liquidity(symbol, start=None):
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
  if start is None:
    start=datetime.today().strftime("%Y-%m-%d")
  batdau = to_timestamp_utc(start, hour=0, minute=0, second=0)
  ketthuc = to_timestamp_utc(start,   hour=0,   minute=0, second=0) + 24*60*59
  sb=[f"{symbol}"]
  payload ={  "from": batdau ,
              "symbols": sb,
              "timeFrame": "ONE_MINUTE",
              "to": ketthuc
            }

  url = "https://trading.vietcap.com.vn/api/chart/v3/OHLCChart/gap-liquidity"
  response = requests.post(url, headers=headers, data=json.dumps(payload))
  data = response.json()[0]
  data=pd.json_normalize(data)
  data = data.explode(['t', 'accumulatedVolume', 'accumulatedValue'])
  data=data[['symbol', 't', 'accumulatedVolume', 'accumulatedValue']]
  mapping={
     'VNINDEX':'HOSE',
     'HNXIndex':'HNX',
     'HNXUpcomIndex':'UPCOM'
  }
  symbolshow=mapping.get(symbol, 'ALL')
  data['symbol']=symbolshow
  data['t'] = data['t'].astype(int).apply(lambda x: datetime.utcfromtimestamp(x))
  data.columns=['symbol','time','vol','val']
  return data
  
def save_liquidity(symbol):
  try:
    mapping={
       'ALL':'ALL',
       'HOSE':'VNINDEX',
       'HNX':'HNXIndex',
       'UPCOM':'HNXUpcomIndex'
    }
    realsymbol = mapping.get(symbol, symbol)
  
    bang=f'"liquidity_{symbol}"'
    enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
    df_db=pd.read_sql(f'SELECT "time" FROM "market_history".{bang}',enginedb)
    data=liquidity(realsymbol)
    existing_dates = set(df_db["time"])
    new_data = data[~data["time"].isin(existing_dates)].copy()
    new_data.to_sql(name=f'liquidity_{symbol}',
                           schema='market_history',
                           con=enginedb,
                           if_exists='append',
                           index=False
                                )
    logging.info(f'Đã lưu liquidity_{symbol}')
  except Exception as E:
    logging.exception(f'Lỗi lưu liquidity_{symbol}')
symbols=['ALL','HOSE','HNX','UPCOM']
def main():
    for sym in symbols:
        save_liquidity(sym)
if __name__=='__main__':
    main()
