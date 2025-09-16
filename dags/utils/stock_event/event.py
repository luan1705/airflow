import requests
import pandas as pd
import json
from pandas import json_normalize
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_event(symbol):
    headers = {
        'authority': 'api.finpath.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'origin': 'https://finpath.vn',
        'referer': 'https://finpath.vn/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
    }
    now=int(datetime.today().timestamp()*1000)

    url = f'https://api.finpath.vn/api/tradingview/events/{symbol.upper()}?start=0&end={now}&resolution=1d'
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        events = response.json().get('data', {}).get('events', [])
        if not events:
            return pd.DataFrame(columns=["symbol", "time", "event","label"])

        data=pd.json_normalize(events)
        data[["symbol", "event"]] = data["text"].str.split(" - ", n=1, expand=True)
        data['time']=pd.to_datetime(data['time']).dt.date
        return data[["symbol", "time", "event","label"]]

    except Exception as e:
        # nếu có lỗi (API hỏng, symbol sai...) vẫn trả df rỗng
        return pd.DataFrame(columns=["symbol", "time", "event","label"])