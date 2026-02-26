# ==== Minimal runnable cell ====
import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
from utils.create_list.symbol_list import indices, DERIVATIVES

def ssi_tradingview_1D(symbol):
    BASE = "https://fc-data.ssi.com.vn/api/v2/Market"

    def get_access_token(consumer_id: str, consumer_secret: str, debug: bool=False) -> str:
        url = f"{BASE}/AccessToken"
        r = requests.post(
            url,
            json={"consumerID": consumer_id, "consumerSecret": consumer_secret},
            headers={"Accept":"application/json","Content-Type":"application/json"},
            timeout=15
        )
        # if debug:
            # print("TOKEN CODE:", r.status_code)
            # print("TOKEN BODY:", r.text[:600])
        r.raise_for_status()
        js = r.json() or {}
        # đọc cả 2 format: data.accessToken hoặc accessToken ở top-level
        token = (js.get("data") or {}).get("accessToken") or js.get("accessToken")
        if not token:
            raise RuntimeError(f"Không thấy accessToken trong phản hồi: {js}")
        return token

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
            df=df[["Symbol","TradingDate","Open","Close","High","Low","Volume"]]
            df.columns = ["symbol","time","open","close","high","low","volume"]
        for col in ["open","close","high","low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if symbol not in indices and symbol not in DERIVATIVES:
                df[col] /= 1000
        return df

    # ==== Điền ID/SECRET của bạn rồi chạy ====
    CONSUMER_ID = "3b312076e1ce40a6b886dce736bd3db5"
    CONSUMER_SECRET = "e213990f319243cf8ae029afb4a123bb"
    end=(datetime.now()-timedelta(days=0)).strftime("%d/%m/%Y")
    start=(datetime.now()-timedelta(days=0)).strftime("%d/%m/%Y")
    token = get_access_token(CONSUMER_ID, CONSUMER_SECRET, debug=True)
    rows  = fetch_daily_ohlc(token, symbol, start, end, page_index=1, page_size=9999)
    df = to_df(rows)
    return df

# if __name__ == "__main__":
#     symbol = "ACB"
#     df = ssi_tradingview_1D(symbol)
#     print(df)