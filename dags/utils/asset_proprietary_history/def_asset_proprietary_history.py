import requests
import pandas as pd
import json
from pandas import json_normalize
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

def proprietary_history(symbol,time,start=None,end=None):
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
            "size": 5000
        }
        url=f'https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol.upper()}/proprietary-history'
        response=requests.get(url,headers=headers,params=params)
        try:
            raw = response.json()['data']['content']
        except Exception:
            raw = []
        if not raw:
            # Schema rỗng
            cols_schema = [
                'Mã CP','Thời điểm GD',
                'KLGD khớp lệnh ròng','GTGD khớp lệnh ròng',
                'KLGD khớp lệnh mua','GTGD khớp lệnh mua',
                'KLGD khớp lệnh bán','GTGD khớp lệnh bán',
                'KLGD thoả thuận ròng','GTGD thoả thuận ròng',
                'KLGD thoả thuận mua','GTGD thoả thuận mua',
                'KLGD thoả thuận bán','GTGD thoả thuận bán',
                'Tổng KLGD ròng','Tổng GTGD ròng',
                'Tổng KLGD mua','Tỷ lệ KLGD mua (%)',
                'Tổng GTGD mua','Tỷ lệ GTGD mua (%)',
                'Tổng KLGD bán','Tỷ lệ KLGD bán (%)',
                'Tổng GTGD bán','Tỷ lệ GTGD bán (%)'
            ]
            return pd.DataFrame(columns=cols_schema)
        data = json_normalize(raw)
        cols_volval=['totalMatchTradeNetVolume',
                         'totalMatchTradeNetValue',
                         'totalMatchBuyTradeVolume',
                         'totalMatchBuyTradeValue',
                         'totalMatchSellTradeVolume',
                         'totalMatchSellTradeValue',
                         'totalDealTradeNetVolume',
                         'totalDealTradeNetValue',
                         'totalDealBuyTradeVolume',
                         'totalDealBuyTradeValue',
                         'totalDealSellTradeVolume',
                         'totalDealSellTradeValue',
                         'totalTradeNetVolume',
                         'totalTradeNetValue',
                         'totalBuyTradeVolume',
                         'percentBuyTradeVolume',
                         'totalBuyTradeValue',
                         'percentBuyTradeValue',
                         'totalSellTradeVolume',
                         'percentSellTradeVolume',
                         'totalSellTradeValue',
                         'percentSellTradeValue'
                         ]
        cols_percent=['percentBuyTradeVolume',
                      'percentBuyTradeValue',
                      'percentSellTradeVolume',
                      'percentSellTradeValue'
                    ]
        int_cols=list(set(cols_volval)-set(cols_percent))
        cols_prop=['ticker','tradingDate'] + cols_volval
        data=data[cols_prop]
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
        data [cols_percent] = data [cols_percent].apply(lambda x:round(x*100,2))
        data [int_cols] = data [int_cols].fillna(0).round(0).astype(int)
        rename_prop={
            'ticker':'Mã CP',
            'tradingDate':'Thời điểm GD',
            'totalMatchTradeNetVolume': 'KLGD khớp lệnh ròng',
            'totalMatchTradeNetValue': 'GTGD khớp lệnh ròng',
            'totalMatchBuyTradeVolume': 'KLGD khớp lệnh mua',
            'totalMatchBuyTradeValue': 'GTGD khớp lệnh mua',
            'totalMatchSellTradeVolume': 'KLGD khớp lệnh bán',
            'totalMatchSellTradeValue': 'GTGD khớp lệnh bán',

            'totalDealTradeNetVolume': 'KLGD thoả thuận ròng',
            'totalDealTradeNetValue': 'GTGD thoả thuận ròng',
            'totalDealBuyTradeVolume': 'KLGD thoả thuận mua',
            'totalDealBuyTradeValue': 'GTGD thoả thuận mua',
            'totalDealSellTradeVolume': 'KLGD thoả thuận bán',
            'totalDealSellTradeValue': 'GTGD thoả thuận bán',

            'totalTradeNetVolume': 'Tổng KLGD ròng',
            'totalTradeNetValue': 'Tổng GTGD ròng',
            'totalBuyTradeVolume': 'Tổng KLGD mua',
            'percentBuyTradeVolume': 'Tỷ lệ KLGD mua (%)',
            'totalBuyTradeValue': 'Tổng GTGD mua',
            'percentBuyTradeValue': 'Tỷ lệ GTGD mua (%)',
            'totalSellTradeVolume': 'Tổng KLGD bán',
            'percentSellTradeVolume': 'Tỷ lệ KLGD bán (%)',
            'totalSellTradeValue': 'Tổng GTGD bán',
            'percentSellTradeValue': 'Tỷ lệ GTGD bán (%)'
        }
        data=data.rename(columns=rename_prop)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        return data
    except Exception as e :
            print(f"Lỗi: {e}")
            return pd.DataFrame()