import pandas as pd
import numpy as np
import requests
import json
from pandas import json_normalize
from datetime import datetime, timedelta,timezone

def to_timestamp_utc(date_str: str, hour=0, minute=0, second=0) -> int:
    """Convert 'YYYY-MM-DD' + (hour:minute:second) UTC -> Unix timestamp (seconds)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc, hour=hour, minute=minute, second=second, microsecond=0)
    return int(dt.timestamp())

def foreign_history(symbol, start, end=None, window_start_utc=2, window_end_utc=8):
  if end is None:
     end=datetime.today().strftime("%Y-%m-%d")
     
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
  
  batdau = to_timestamp_utc(start, hour=window_start_utc, minute=0, second=0)
  ketthuc = to_timestamp_utc(end,   hour=window_end_utc,   minute=0, second=0)

  payload ={
      "from": batdau,
      "to": ketthuc,
      "group": f"{symbol}",
      "timeFrame": "ONE_DAY"
  }

  url = "https://trading.vietcap.com.vn/api/market-watch/v3/ForeignVolumeChart/getAll"
  response = requests.post(url, headers=headers, data=json.dumps(payload))
  data = response.json()
  data=json_normalize(data)
  data=data.drop(columns=['group','dataType','timeFrame'])
  data.rename(columns={'foreignBuyVolume':'buyVol','foreignSellVolume':'sellVol','foreignBuyValue':'buyVal','foreignSellValue':'sellVal','truncTime':'time'},inplace=True)
  data['time']=data['time'].astype(float)
  data['time'] = pd.to_datetime(data['time'], unit='s')
  cols=['buyVol','sellVol','buyVal','sellVal']
  data[cols]=data[cols].apply(pd.to_numeric, errors='coerce').astype(int)
  data['netVol']=data['buyVol'].astype(int)-data['sellVol'].astype(int)
  data['netVal']=data['buyVal'].astype(int)-data['sellVal'].astype(int)
  data=data[['time','buyVol','sellVol','netVol','buyVal','sellVal','netVal']]
  
  return data
