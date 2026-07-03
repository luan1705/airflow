# ==== Minimal runnable cell ====
import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
from utils.create_list.symbol_list import indices, DERIVATIVES

BASE = "https://fc-data.ssi.com.vn/api/v2/Market"
def get_access_token():
    url = f"{BASE}/AccessToken"
    r = requests.post(
        url,
        json={
            "consumerID": "14d9af7cd5c447f0a26c2c43218ed464",
            "consumerSecret": "940870b0e2134685a2b344fa33339842"
        },
        headers={"Accept":"application/json","Content-Type":"application/json"},
        timeout=15
    )
    r.raise_for_status()
    js = r.json()
    return (js.get("data") or {}).get("accessToken")

def ssi_tradingview_1D(symbol,token):
    def fetch_daily_ohlc(token: str, symbol: str, from_ddmmyyyy: str, to_ddmmyyyy: str,
                        page_index: int = 1, page_size: int = 1000):
        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "symbol": symbol,
            "fromDate": from_ddmmyyyy,   # dạng dd/MM/yyyy
            "toDate":   to_ddmmyyyy,     # dạng dd/MM/yyyy
            "pageIndex": page_index,     # 1..10 (theo spec)
            "pageSize":  page_size       # 10|20|50|100|1000
        }
        r = requests.get(f"{BASE}/DailyOhlc", headers=headers, params=params, timeout=30)
        # print("DATA CODE:", r.status_code)
        # print("DATA URL :", r.url)
        r.raise_for_status()
        js = r.json() or {}
        return js.get("data") or []

    def to_df(rows):
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        if "TradingDate" in df.columns:
            df["TradingDate"] = pd.to_datetime(df["TradingDate"], format="%d/%m/%Y", dayfirst=True, errors="coerce")
            df = df.sort_values(by=["TradingDate"],ascending=False).reset_index(drop=True)
            df=df[["Symbol","TradingDate","Open","Close","High","Low","Volume","Value"]]
            df.columns = ["symbol","time","open","close","high","low","volume","value"]
        for col in ["open","close","high","low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if symbol not in indices and symbol not in DERIVATIVES:
                df[col] /= 1000
        return df

    # ==== Điền ID/SECRET của bạn rồi chạy ====
    # CONSUMER_ID = "3b312076e1ce40a6b886dce736bd3db5"
    # CONSUMER_SECRET = "e213990f319243cf8ae029afb4a123bb"
    end=(datetime.now()-timedelta(days=0)).strftime("%d/%m/%Y")
    start=(datetime.now()-timedelta(days=5)).strftime("%d/%m/%Y")
    # TOKEN = get_access_token()
    rows  = fetch_daily_ohlc(token, symbol, start, end, page_index=1, page_size=1000)
    df = to_df(rows)
    return df

# #------------------------------test------------------------------
# indices = ["VNINDEX","HNXINDEX","UPCOMINDEX","VN30","HNX30"]
# DERIVATIVES = ["FUTURES1M","FUTURES2M","FUTURES3M","FUTURES6M"]
# if __name__ == "__main__":
#     symbol = "ABS"
#     token = get_access_token()
#     df = ssi_tradingview_1D(symbol, token)
#     print(df)