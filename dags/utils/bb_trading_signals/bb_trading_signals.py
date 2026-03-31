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
        Buy = score == 3
    else:
        Buy = score >= 2

    buyShape = np.where(Buy, L, np.nan)
    bgVal = Buy.astype(int)

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

    df["buy"] = Buy
    df["buyShape"] = buyShape
    df["bgVal"] = bgVal
    # df["signal"] = np.where(df["buy"], "BUY", None)

    # df = df[df["buy"]].copy()
    # if df.empty:
    #     return df

    latest_date = df["time"].dt.date.max()
    df = df[df["time"].dt.date == latest_date].copy()

    return df


def save_pg(symbol):
    symbol = symbol.upper()
    df = bb_trading_signals(symbol)

    if df.empty:
        return f"⚠ Không có dữ liệu bb trading {symbol}"

    df["symbol"] = symbol

    cols_order = [
        "symbol", "time", "open", "close", "high", "low", "volume",
        "bb_top", "bb_bot", "bb_mid",
        "DBKL", "DKBB", "DBG_pct", "VTB",
        "DBG_cond", "DKBB_cond", "DKKL_cond",
        "DBG_val", "DKBB_val", "DKKL_val",
        "score", "buy", "buyShape", "bgVal"
    ]
    df = df[cols_order]

    table = "signals.bb_trading_signals"

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        symbol TEXT PRIMARY KEY,
        time TIMESTAMPTZ,
        open DOUBLE PRECISION,
        close DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        bb_top DOUBLE PRECISION,
        bb_bot DOUBLE PRECISION,
        bb_mid DOUBLE PRECISION,
        "DBKL" DOUBLE PRECISION,
        "DKBB" DOUBLE PRECISION,
        "DBG_pct" DOUBLE PRECISION,
        "VTB" DOUBLE PRECISION,
        "DBG_cond" BOOLEAN,
        "DKBB_cond" BOOLEAN,
        "DKKL_cond" BOOLEAN,
        "DBG_val" DOUBLE PRECISION,
        "DKBB_val" DOUBLE PRECISION,
        "DKKL_val" DOUBLE PRECISION,
        score DOUBLE PRECISION,
        buy BOOLEAN,
        "buyShape" DOUBLE PRECISION,
        "bgVal" DOUBLE PRECISION
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

        cols = list(df.columns)
        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

        insert_sql = f"""
        INSERT INTO {table} ("{'","'.join(cols)}")
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET
            time = EXCLUDED.time,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            volume = EXCLUDED.volume,
            bb_top = EXCLUDED.bb_top,
            bb_bot = EXCLUDED.bb_bot,
            bb_mid = EXCLUDED.bb_mid,
            "DBKL" = EXCLUDED."DBKL",
            "DKBB" = EXCLUDED."DKBB",
            "DBG_pct" = EXCLUDED."DBG_pct",
            "VTB" = EXCLUDED."VTB",
            "DBG_cond" = EXCLUDED."DBG_cond",
            "DKBB_cond" = EXCLUDED."DKBB_cond",
            "DKKL_cond" = EXCLUDED."DKKL_cond",
            "DBG_val" = EXCLUDED."DBG_val",
            "DKBB_val" = EXCLUDED."DKBB_val",
            "DKKL_val" = EXCLUDED."DKKL_val",
            score = EXCLUDED.score,
            buy = EXCLUDED.buy,
            "buyShape" = EXCLUDED."buyShape",
            "bgVal" = EXCLUDED."bgVal"
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
    return result