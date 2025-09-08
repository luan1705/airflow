import requests
import pandas as pd
import json
from pandas import json_normalize
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import ReadTimeout, ConnectionError, RequestException, HTTPError

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=5, max=60),
    retry=retry_if_exception_type((ReadTimeout, ConnectionError, HTTPError))
)
def fetch_vietcap_ohlc(symbol, payload, headers):
    url = "https://trading.vietcap.com.vn/api/chart/OHLCChart/gap-chart"
    response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    
    # Gây lỗi HTTPError để retry nếu gặp 429
    if response.status_code == 429:
        print(f"⏳ Too Many Requests (429) – sẽ retry symbol={symbol}")
        raise HTTPError("Too Many Requests (429)")

    response.raise_for_status()
    return response

def tradingview(symbol, start, end, time="days"):
    headers = {
        'host': 'trading.vietcap.com.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'origin': 'https://trading.vietcap.com.vn',
        'referer': 'https://trading.vietcap.com.vn/?filter-group=HOSE&filter-value=HOSE&view-type=FLAT&type=stock',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/135.0.0.0 Safari/537.36'
    }

    end_dt = datetime.strptime(end, '%Y-%m-%d') + timedelta(days=1)
    end_timestamp = end_dt.timestamp()

    time_dict = {
        'days': 'ONE_DAY',
        'months': 'ONE_MONTH',
        'minutes': 'ONE_MINUTE',
        'hours': 'ONE_HOUR'
    }

    payload = {
        "timeFrame": time_dict[time],
        "symbols": [symbol],
        "countBack": 30000,
        "to": end_timestamp
    }

    try:
        response = fetch_vietcap_ohlc(symbol, payload, headers)

        if response.status_code != 200:
            print(f"❌ HTTP {response.status_code} - symbol={symbol}")
            return pd.DataFrame()

        if not response.content or response.text.strip() == "":
            print(f"❌ Rỗng response cho symbol={symbol}")
            return pd.DataFrame()

        json_data = response.json()
        if not json_data or not isinstance(json_data, list) or len(json_data) == 0:
            print(f"❌ Không có dữ liệu trong JSON với symbol={symbol}")
            return pd.DataFrame()

        data = json_data[0]

    except Exception as e:
        print(f"❌ Lỗi trong truy xuất dữ liệu cho symbol={symbol} - {e}")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df = df[["symbol", "t", "o", "c", "h", "l", "v"]]
    df["t"] = pd.to_numeric(df['t'], errors='coerce')
    df["t"] = df['t'].apply(lambda x: datetime.fromtimestamp(x))
    df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]].apply(lambda x: round(x / 1000, 2))
    
    if time in ['minutes', 'hours']:
        df["t"] = pd.to_datetime(df["t"]).dt.tz_localize(None).dt.floor("s")
    elif time in ['days', 'months']:
        df["t"] = pd.to_datetime(df["t"]).dt.tz_localize(None).dt.floor("d")

    df.rename(columns={
        "symbol": "symbol",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "t": "time",
        "v": "volume"
    }, inplace=True)

    return df[df["time"] >= start]
