import requests
import pandas as pd
import json
from pandas import json_normalize
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

def foreign_history(symbol,time,start=None,end=None):
    try:
        headers = {
        'host': 'iq.vietcap.com.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'origin': 'https://trading.vietcap.com.vn',
        'referer': 'https://trading.vietcap.com.vn/',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }

        time_dict= {
            'days': 'ONE_DAY',
            'months': 'ONE_MONTH',
            'quarters': 'ONE_QUARTER',
            'years': 'ONE_YEAR'
            }
        if time=='months':
            start = start + '-01'
            end = end + '-01'
        elif time=='quarters':
            start = start + '-01-01'
            end = end + '-12-31'
        elif time=='years':
            start = start + '-01-01'
            end = end + '-01-01'
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')
        start_date = start.strftime('%Y%m%d')
        end_date = end.strftime('%Y%m%d')
        params = {
            "timeFrame": time_dict[time] ,
            "fromDate": start_date,
            "toDate": end_date,
            "size": 50000
        }
        url=f'https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol.upper()}/price-history'
        response=requests.get(url,headers=headers,params=params)

        try:
            raw = response.json()['data']['content']
        except Exception:
            raw = []

        if not raw:
            # DataFrame rỗng nhưng có sẵn schema
            cols_schema = [
                'Mã CP','Thời điểm GD','KLGD khớp lệnh ròng ','GTGD khớp lệnh ròng ',
                'KLGD khớp lệnh mua ','GTGD khớp lệnh mua ','KLGD khớp lệnh bán ','GTGD khớp lệnh bán ',
                'KLGD thoả thuận ròng ','GTGD thoả thuận ròng ','KLGD thoả thuận mua ','GTGD thoả thuận mua ',
                'KLGD thoả thuận bán ','GTGD thoả thuận bán ','Tổng KLGD ròng','Tổng GTGD ròng',
                'Tổng KLGD mua','Tổng GTGD mua','Tổng KLGD bán','Tổng GTGD bán',
                '% Khối ngoại tối đa','% Khối ngoại sở hữu','% Khối ngoại còn lại','CP còn lại'
            ]
            data = pd.DataFrame(columns=cols_schema)
        else:
            data = json_normalize(raw)

            cols_amount=['foreignNetVolumeMatched',
                       'foreignNetValueMatched',
                       'foreignBuyVolumeMatched',
                       'foreignBuyValueMatched',
                       'foreignSellVolumeMatched',
                       'foreignSellValueMatched',
                       'foreignNetVolumeDeal',
                       'foreignNetValueDeal',
                       'foreignBuyVolumeDeal',
                       'foreignBuyValueDeal',
                       'foreignSellVolumeDeal',
                       'foreignSellValueDeal',
                       'foreignNetVolumeTotal',
                       'foreignNetValueTotal',
                       'foreignBuyVolumeTotal',
                       'foreignBuyValueTotal',
                       'foreignSellVolumeTotal',
                       'foreignSellValueTotal']
            cols_percent=['foreignRoomPercentage',   #(%)
                           'foreignOwnedPercentage',  #(%)
                           'foreignAvailablePercentage'] #(%)
            cols_data=['ticker','tradingDate']+cols_amount+cols_percent+['foreignCurrentRoom']
            for col in cols_data:
                if col not in data.columns:
                    data[col] = None 
            data=data[cols_data]
            data['tradingDate'] = pd.to_datetime(data['tradingDate'])
            quy={'1': 'Q1',
                 '4': 'Q2',
                 '7': 'Q3',
                 '10': 'Q4'}
            if time=='months':
                data['tradingDate'] = data['tradingDate'].dt.strftime('%Y-%m')
            elif time=='quarters':
                data['tradingDate']=(data['tradingDate'].dt.month.map(lambda m:quy.get(str(m),'')))+' '+data['tradingDate'].dt.year.astype(str)
            elif time=='years':
                data['tradingDate'] = data['tradingDate'].dt.strftime('%Y').astype(int)
            data [cols_amount+['foreignCurrentRoom']] = data [cols_amount+['foreignCurrentRoom']].astype(int)
            data[cols_percent] = data[cols_percent].apply(lambda x: round(x*100,2))
            rename_foreign = {
                'ticker': 'Mã CP',
                'tradingDate': 'Thời điểm GD',
                'foreignNetVolumeMatched': 'KLGD khớp lệnh ròng ',
                'foreignNetValueMatched': 'GTGD khớp lệnh ròng ',
                'foreignBuyVolumeMatched': 'KLGD khớp lệnh mua ',
                'foreignBuyValueMatched': 'GTGD khớp lệnh mua ',
                'foreignSellVolumeMatched': 'KLGD khớp lệnh bán ',
                'foreignSellValueMatched': 'GTGD khớp lệnh bán ',
                'foreignNetVolumeDeal': 'KLGD thoả thuận ròng ',
                'foreignNetValueDeal': 'GTGD thoả thuận ròng ',
                'foreignBuyVolumeDeal': 'KLGD thoả thuận mua ',
                'foreignBuyValueDeal': 'GTGD thoả thuận mua ',
                'foreignSellVolumeDeal': 'KLGD thoả thuận bán ',
                'foreignSellValueDeal': 'GTGD thoả thuận bán ',
                'foreignNetVolumeTotal': 'Tổng KLGD ròng',
                'foreignNetValueTotal': 'Tổng GTGD ròng',
                'foreignBuyVolumeTotal': 'Tổng KLGD mua',
                'foreignBuyValueTotal': 'Tổng GTGD mua',
                'foreignSellVolumeTotal': 'Tổng KLGD bán',
                'foreignSellValueTotal': 'Tổng GTGD bán',
                'foreignRoomPercentage': '% Khối ngoại tối đa',
                'foreignOwnedPercentage': '% Khối ngoại sở hữu',
                'foreignAvailablePercentage': '% Khối ngoại còn lại',
                'foreignCurrentRoom': 'CP còn lại'
            }
            data=data.rename(columns=rename_foreign)
        return data
    except Exception as e :
        print(f"Lỗi: {e}")
        return pd.DataFrame()