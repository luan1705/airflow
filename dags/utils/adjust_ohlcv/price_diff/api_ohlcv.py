import requests
from datetime import datetime, timedelta
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.ssi_ohlcv.ssi_v3_key import API_KEY, API_SECRET
from utils.create_list.symbol_list import indices, DERIVATIVES

# ============================================================
# SESSION
# ============================================================

session = requests.Session()

retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503],
    allowed_methods=["GET", "POST"],
    raise_on_status=False
)

adapter = HTTPAdapter(max_retries=retry)

session.mount("https://", adapter)
session.mount("http://", adapter)

# ============================================================
# CONFIG
# ============================================================

BASE = "https://api.ssi.com.vn/api/v3"

# ============================================================
# ACCESS TOKEN
# ============================================================

def get_access_token():
    url = f"{BASE}/auth/token"

    r = session.post(
        url,
        json={"apiKey": API_KEY, "apiSecret": API_SECRET},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=15
    )

    r.raise_for_status()

    js = r.json() or {}
    token = js.get("accessToken")

    if not token:
        raise ValueError(f"SSI không trả về accessToken. Response: {js}")

    return token

# ============================================================
# DAILY OHLC
# ============================================================

def ssi_tradingview_1D(symbol, token):

    def fetch_daily_ohlc(token, symbol, from_date, to_date, page_index=1, page_size=1000):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}

        params = {
            "symbol": symbol,
            "from": from_date,
            "to": to_date,
            "timeFrame": "1d",
            "pageIndex": page_index,
            "pageSize": page_size
        }

        r = session.get(f"{BASE}/data/ohlc", headers=headers, params=params, timeout=(5, 60))
        r.raise_for_status()

        js = r.json() or {}
        return js.get("data") or []

    # ========================================================
    # CONVERT API RESPONSE -> DATAFRAME
    # ========================================================

    def to_df(rows):
        df = pd.DataFrame(rows)

        if df.empty:
            return df

        required_columns = ["tradingDate", "symbol", "open", "close", "high", "low", "volume"]
        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            raise ValueError(f"SSI v3 thiếu columns: {missing}. Columns nhận được: {list(df.columns)}")

        df["tradingDate"] = pd.to_datetime(df["tradingDate"], errors="coerce")

        df = df[["symbol", "tradingDate", "open", "close", "high", "low", "volume"]]
        df.columns = ["symbol", "time", "open", "close", "high", "low", "volume"]

        for col in ["open", "close", "high", "low", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if symbol not in indices and symbol not in DERIVATIVES:
            df[["open", "close", "high", "low"]] /= 1000

        return df

    # ========================================================
    # DATE RANGE
    # ========================================================

    end = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d 23:59:59")
    start = (datetime.now() - timedelta(days=30)).strftime("%Y/%m/%d 00:00:00")

    # ========================================================
    # FETCH
    # ========================================================

    rows = fetch_daily_ohlc(
        token=token,
        symbol=symbol,
        from_date=start,
        to_date=end,
        page_index=1,
        page_size=1000
    )

    if not rows:
        return pd.DataFrame()

    # ========================================================
    # DATAFRAME
    # ========================================================

    df = to_df(rows)

    if df.empty:
        return df

    df = (
        df.dropna(subset=["time"])
        .sort_values("time")
        .drop_duplicates(subset=["time"], keep="last")
        .reset_index(drop=True)
    )

    return df