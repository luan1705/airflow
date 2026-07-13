import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta


def get_event(symbol):
    headers = {
        'authority': 'wlgw-technical.fiintrade.vn',
        'content-type': 'application/json',
        'origin': 'https://fx.kafi.vn',
        'referer': 'https://fx.kafi.vn/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
    }

    from_date = "2000-01-01T00:00:00.000Z"
    to_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    # from_date = (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    params = {
        'language': 'vi',
        'OrganCode': symbol.upper(),
        'From': from_date,
        'To': to_date,
    }

    url = 'https://wlgw-technical.fiintrade.vn/TradingView/GetStockEvents'

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        items = response.json().get('items', [])

        if not items:
            return pd.DataFrame(columns=['symbol', 'time', 'label', 'valuePerShare', 'exerciseRate', 'issueVolume', 'exerciseRatio', 'revenue', 'profit', 'lengthReport', 'yearReport'])

        data = pd.json_normalize(items)
        data['exrightDate'] = pd.to_datetime(data.get('exrightDate'), format='mixed', errors='coerce').dt.date if 'exrightDate' in data.columns else None
        data['publicDate']  = pd.to_datetime(data['publicDate'], format='mixed', errors='coerce').dt.date
        data['time']        = data['exrightDate'].fillna(data['publicDate']) if 'exrightDate' in data.columns else data['publicDate']
        data['symbol']      = symbol

        data = data[data['type'].isin(['Dividend', 'Earning', 'ShareIssuance'])]
        data['label'] = data['type'].map({
            'Earning':       'F',
            'Dividend':      'D',
            'ShareIssuance': 'S',
        })

        columns = ['symbol', 'time', 'label', 'valuePershare', 'exerciseRate', 'issueVolumn', 'exerciseRatio', 'revenue', 'profit', 'lengthReport', 'yearReport']
        data = data.reindex(columns=columns)
        data = data.rename(columns={'issueVolumn': 'issueVolume', 'valuePershare': 'valuePerShare'})

        int_cols = ['revenue', 'profit', 'issueVolume', 'valuePerShare', 'lengthReport', 'yearReport']
        data[int_cols] = data[int_cols].apply(pd.to_numeric, errors='coerce').astype('Int64')

        return data

    except Exception as e:
        return pd.DataFrame(columns=['symbol', 'time', 'label', 'valuePerShare', 'exerciseRate', 'issueVolume', 'exerciseRatio', 'revenue', 'profit', 'lengthReport', 'yearReport'])


if __name__ == "__main__":
    df = get_event("ACB")
    print(df)