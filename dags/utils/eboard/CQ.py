import pandas as pd
import numpy as np
import json
import requests
from pandas import json_normalize
from datetime import datetime
from sqlalchemy import create_engine
import logging

log=logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    pool_size=10,
    max_overflow=20,
    pool_timeout=60
    )
    

def save_cq():
    headers={
       'authority': 'api.fmarket.vn',
       'accept': 'application/json, text/plain, */*',
       'accept-encoding': 'gzip, deflate, br, zstd',
       'accept-language': 'en-US,en;q=0.9',
       'content-type': 'application/json',
       'origin': 'https://fmarket.vn',
       'referer': 'https://fmarket.vn/',
       'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
       'sec-ch-ua-mobile': '?0',
       'sec-ch-ua-platform': '"Windows"',
       'sec-fetch-dest': 'empty',
       'sec-fetch-mode': 'cors',
       'sec-fetch-site': 'same-origin',
       'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
     }

    url=f'https://iboard-query.ssi.com.vn/stock/cw/hose'
    response=requests.get(url,headers=headers)
    data=pd.json_normalize(response.json()['data']['coveredWarrantData'])
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    data=data[['stockSymbol','issuerName','lastTradingDate']]
    data['lastTradingDate']=pd.to_datetime(data['lastTradingDate'], format="%Y%m%d")
    data.columns=['symbol','TCPH','GDCC']


    data.to_sql(
        name='CQ',
        con=engine,
        schema = 'history_data',
        if_exists='replace',
        index=False,
        chunksize= 800 
        )
    log.info(f'Đã cập nhật CQ')
            