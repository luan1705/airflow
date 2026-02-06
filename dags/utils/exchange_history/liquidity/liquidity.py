import pandas as pd
import numpy as np
import requests
import json
from pandas import json_normalize
from datetime import datetime,timezone,timedelta
import logging
from sqlalchemy import create_engine, text
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
  ketthuc = to_timestamp_utc(start,   hour=0,   minute=0, second=0) + 24*60*60 - 1
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
  data['t'] = pd.to_datetime(data['t'].astype(int), unit='s', utc=True).dt.tz_convert('Asia/Ho_Chi_Minh')
  data.columns=['symbol','time','vol','val']
  return data
  
def save_liquidity(symbol,enginedb):
  try:
    mapping={
       'HOSE':'VNINDEX',
       'HNX':'HNXIndex',
       'UPCOM':'HNXUpcomIndex'
    }
    realsymbol = mapping.get(symbol, symbol)
    bang=f'"liquidity_{symbol}"'
    df_db=pd.read_sql(f'SELECT "time" FROM "exchange_history".{bang}',enginedb)
    data=liquidity(realsymbol)
    existing_dates = set(df_db["time"])
    new_data = data[~data["time"].isin(existing_dates)].copy()
    if not new_data.empty:
      new_data.to_sql(
        name=f'liquidity_{symbol}',
        schema='exchange_history',
        con=enginedb,
        if_exists='append',
        index=False
      )
      logging.info(f'Đã append {len(new_data)} rows liquidity_{symbol}')
    else:
      logging.info(f'Không có phút mới để append liquidity_{symbol}')

    # (B) ✅ UPSERT đúng 1 dòng lúc 15:00 (giờ VN)
    today_vn = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").normalize()
    target_time = today_vn + pd.Timedelta(hours=15)  # 15:00:00+07

    row_1500 = data[data["time"] == target_time]
    if not row_1500.empty:
      r = row_1500.iloc[0]
      upsert_sql = text(f"""
        INSERT INTO "exchange_history".{bang} ("symbol","time","vol","val")
        VALUES (:symbol,:time,:vol,:val)
        ON CONFLICT ("time")
        DO UPDATE SET
          "vol" = EXCLUDED."vol",
          "val" = EXCLUDED."val",
          "symbol" = EXCLUDED."symbol";
      """)
      with enginedb.begin() as conn:
        conn.execute(upsert_sql, {
          "symbol": r["symbol"],
          "time":   r["time"].to_pydatetime(),
          "vol":    int(r["vol"]) if pd.notna(r["vol"]) else None,
          "val":    float(r["val"]) if pd.notna(r["val"]) else None,
        })
      logging.info(f"♻️ Upsert 15:00 OK for liquidity_{symbol}: {target_time}")
    else:
      logging.info(f"Chưa có dòng 15:00 trong API (liquidity_{symbol}) nên không upsert")
  except Exception:
    logging.exception(f"Lỗi lưu liquidity_{symbol}")
symbols=['HOSE','HNX','UPCOM']
def main():
    enginedb = create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")
    try:
        for sym in symbols:
            save_liquidity(sym,enginedb)
    finally:
        enginedb.dispose()
        logging.info("🔒 Đã đóng kết nối DB")