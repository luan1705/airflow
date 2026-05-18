import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
import concurrent.futures
import logging
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE, indices, addition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)

today = datetime.now().date()
batdau = today - timedelta(days=100)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
)

def bb_trading_signals(
    symbol,
    source="close",     # open, high, low, close, hl2, hlc3, ohlc4
    bbLen=21,
    bbMult=2.0,
    dbgPct=1.5,         # % tăng so với hôm trước
    dbklLen=21,
    vtbLen=20,
    vtbMin=100000,
    dkbbLen=21,
    dkbbThr=-39,
    buyMode=0,          # 0 = >=2/3, 1 = 3/3
):
    query = f"""
    SELECT *
    FROM ohlcv."{symbol}_1D"
    ORDER BY time desc
    LIMIT 200
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        log.warning(f"⚠ Không có bảng ohlcv.{symbol}_1D")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
    df = df.sort_values("time", ascending=True).reset_index(drop=True)

    O = df["open"].astype(float)
    H = df["high"].astype(float)
    L = df["low"].astype(float)
    C = df["close"].astype(float)
    V = df["volume"].astype(float)

    # Source giống TradingView
    if source == "open":
        src = O
    elif source == "high":
        src = H
    elif source == "low":
        src = L
    elif source == "hl2":
        src = (H + L) / 2
    elif source == "hlc3":
        src = (H + L + C) / 3
    elif source == "ohlc4":
        src = (O + H + L + C) / 4
    else:
        src = C

    # ===== Bollinger Bands =====
    basis = src.rolling(bbLen, min_periods=bbLen).mean()
    sd = src.rolling(bbLen, min_periods=bbLen).std(ddof=0)
    dev = sd * bbMult
    upper = basis + dev
    lower = basis - dev

    # ===== DBKL =====
    vma = V.rolling(dbklLen, min_periods=dbklLen).mean()
    dbkl = np.where(vma != 0, ((V - vma) / vma) * 100, np.nan)
    dbkl = pd.Series(dbkl, index=df.index)

    # ===== DBG =====
    prevClose = C.shift(1)
    rocPct = np.where(prevClose != 0, ((C - prevClose) / prevClose) * 100, np.nan)
    rocPct = pd.Series(rocPct, index=df.index)
    dbgCond = rocPct > dbgPct

    # ===== VTB / DKKL =====
    vtb = V.rolling(vtbLen, min_periods=vtbLen).mean()
    dkklCond = vtb > vtbMin

    # ===== DKBB =====
    bbWidth = upper - lower
    bbWidthMA = bbWidth.rolling(dkbbLen, min_periods=dkbbLen).mean()
    dkbb = np.where(bbWidthMA != 0, ((bbWidth - bbWidthMA) * 100) / bbWidthMA, np.nan)
    dkbb = pd.Series(dkbb, index=df.index)
    dkbbCond = dkbb <= dkbbThr

    # ===== SCORE =====
    dbgVal = dbgCond.astype(int)
    dkbbVal = dkbbCond.astype(int)
    dkklVal = dkklCond.astype(int)
    score = dbgVal + dkbbVal + dkklVal

    if buyMode == 1:
        signal_cond = score == 3
    else:
        signal_cond = score >= 2

    signal_cond = signal_cond & (C >= 10)

    buyShape = np.where(signal_cond, L, np.nan)
    bgVal = signal_cond.astype(int)

    df["bb_top"] = upper
    df["bb_bot"] = lower
    df["bb_mid"] = basis

    df["DBKL"] = dbkl
    df["DKBB"] = dkbb
    df["DBG_pct"] = rocPct
    df["VTB"] = vtb

    df["DBG_cond"] = dbgCond
    df["DKBB_cond"] = dkbbCond
    df["DKKL_cond"] = dkklCond

    df["DBG_val"] = dbgVal
    df["DKBB_val"] = dkbbVal
    df["DKKL_val"] = dkklVal
    df["score"] = score

    df["signal"] = np.where(signal_cond, "BUY", None)
    df["buyShape"] = buyShape
    df["bgVal"] = bgVal

    df_original = df.copy()
    df = df[df["signal"] == "BUY"].copy()
    if df.empty:
        return df

    # latest_date = df["time"].dt.date.max()
    # df = df[df["time"].dt.date == latest_date].copy()

    if df.empty:
        return pd.DataFrame()

    df = df.sort_values("time")
    last_row = df.iloc[-1]

    buy_date = last_row["time"]
    buy_price = float(last_row["close"])

    # ===== STOPLOSS 8% =====
    stop_price = buy_price * 0.92

    sell_date = None
    sell_price = None

    # dữ liệu sau ngày mua
    after_buy = df_original[df_original["time"] > buy_date].copy()

    for _, row in after_buy.iterrows():

        # nếu giá thấp nhất <= stoploss
        if row["close"] <= stop_price:
            sell_date = row["time"]
            sell_price = row["close"]
            break
    
    trade = {
        "symbol": symbol,
        "buy_date": buy_date,
        "buy_price": buy_price,
        "sell_date": sell_date,
        "sell_price": sell_price
    }

    return pd.DataFrame([trade])


def save_pg(symbol):
    symbol = symbol.upper()
    df = bb_trading_signals(symbol)

    if df.empty:
        return f"⚠ Không có dữ liệu bb trading {symbol}"

    table = "signals.bb_trading_signals"

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        symbol TEXT PRIMARY KEY,
        buy_date TIMESTAMPTZ,
        buy_price DOUBLE PRECISION,
        sell_date TIMESTAMPTZ,
        sell_price DOUBLE PRECISION
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

        cols = ["symbol", "buy_date", "buy_price", "sell_date", "sell_price"]
        rows = [tuple(r) for r in df[cols].itertuples(index=False, name=None)]

        insert_sql = f"""
        INSERT INTO {table} ("{'","'.join(cols)}")
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET
            buy_date = EXCLUDED.buy_date,
            buy_price = EXCLUDED.buy_price,
            sell_date = EXCLUDED.sell_date,
            sell_price = EXCLUDED.sell_price
        """

        execute_values(conn.connection.cursor(), insert_sql, rows, page_size=800)

    return f"✔ {symbol}: {len(df)} rows inserted"


def update_all_symbol(symbol_list):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_pg, sym): sym for sym in symbol_list}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return results


def save_all_pg():
    result = []
    result += update_all_symbol(HOSE)
    result += update_all_symbol(HNX)
    result += update_all_symbol(UPCOM)
    # result += update_all_symbol(DERIVATIVES)
    # result += update_all_symbol(CW)
    # result += update_all_symbol(HNXBOND)
    # result += update_all_symbol(ETFHOSE)
    # result += update_all_symbol(indices)
    # result += update_all_symbol(addition)

    errors = [msg for msg in result if msg.startswith("❌") or msg.startswith("⚠")]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return errors if errors else ["✅ không có lỗi"]