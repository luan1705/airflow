# ==== Minimal runnable cell - SSI API v3 ====
import requests
from datetime import datetime, timedelta
import pandas as pd
from .ssi_v3_key import API_KEY, API_SECRET
from utils.create_list.symbol_list import indices, DERIVATIVES

# #=============chạy trực tiếp==================
# indices=[]
# DERIVATIVES=[]
# #===============================

BASE = "https://api.ssi.com.vn/api/v3"

# ============================================================
# GET ACCESS TOKEN
# ============================================================

def get_access_token():
    url = f"{BASE}/auth/token"
    r = requests.post(url, json={"apiKey": API_KEY, "apiSecret": API_SECRET}, headers={"Accept": "application/json", "Content-Type": "application/json"}, timeout=15)
    r.raise_for_status()
    js = r.json()
    return js.get("accessToken")

# ============================================================
# GET DAILY OHLC
# ============================================================

def ssi_tradingview_1D(symbol, token):

    def fetch_daily_ohlc(token: str, symbol: str, from_date: str, to_date: str, page_index: int = 1, page_size: int = 1000):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}
        params = {"symbol": symbol, "from": from_date, "to": to_date, "timeFrame": "1d", "pageIndex": page_index, "pageSize": page_size}
        r = requests.get(f"{BASE}/data/ohlc", headers=headers, params=params, timeout=30)

        # print("DATA CODE:", r.status_code)
        # print("DATA URL :", r.url)
        # print("DATA RESPONSE:", r.text)

        r.raise_for_status()
        js = r.json() or {}
        return js.get("data") or []

    # ========================================================
    # CONVERT TO DATAFRAME
    # ========================================================

    def to_df(rows):
        df = pd.DataFrame(rows)
        if df.empty:
            return df

        # v3:
        # tradingDate
        # symbol
        # open
        # close
        # high
        # low
        # volume
        # value

        if "tradingDate" in df.columns:
            df["tradingDate"] = pd.to_datetime(df["tradingDate"], format="%Y/%m/%d", errors="coerce")
            df = df.sort_values(by=["tradingDate"], ascending=False).reset_index(drop=True)
            df = df[["symbol", "tradingDate", "open", "close", "high", "low", "volume", "value"]]
            df.columns = ["symbol", "time", "open", "close", "high", "low", "volume", "value"]

        # Convert numeric
        for col in ["open", "close", "high", "low", "volume", "value"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # SSI v3 trả giá dạng:
        # 19942.535
        #
        # Nếu logic database cũ của m đang dùng
        # giá cổ phiếu / 1000 thì giữ nguyên logic này.

        if symbol not in indices and symbol not in DERIVATIVES:
            df[["open", "close", "high", "low"]] /= 1000

        return df

    # ========================================================
    # DATE RANGE
    # ========================================================

    end = datetime.now().strftime("%Y/%m/%d 23:59:59")
    start = (datetime.now() - timedelta(days=5)).strftime("%Y/%m/%d 00:00:00")

    # ========================================================
    # FETCH
    # ========================================================

    rows = fetch_daily_ohlc(token, symbol, start, end, page_index=1, page_size=1000)
    df = to_df(rows)
    return df

# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":
    symbol = "VIC"
    token = get_access_token()
    df = ssi_tradingview_1D(symbol, token)
    print(df)