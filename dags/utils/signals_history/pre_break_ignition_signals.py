import numpy as np
import pandas as pd
from VNSFintech import *
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
import concurrent.futures
import logging
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE, indices, addition

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

today = datetime.now().date()
batdau = today - timedelta(days=50)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
)

# ================== Tính tín hiệu ==================
def pre_break_ignition_signals(symbol, lookback=10, tightLimit=10.0, volBurst=1.3, minVol=50000, downLookback=10):
    query = f"""
    SELECT *
    FROM ohlcv."{symbol}_1D"
    ORDER BY time desc
    LIMIT 200
    """

    try:
        df = pd.read_sql(query, engine)
    except:
        log.warning(f"⚠ Không có bảng ohlcv.{symbol}_1D")
        return pd.DataFrame()
    
    df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
    df = df.sort_values("time", ascending=True).reset_index(drop=True)
    H = df["high"].astype(float)
    L = df["low"].astype(float)
    C = df["close"].astype(float)
    V = df["volume"].astype(float)
    v20 = V.rolling(20).mean()

    HHV_now = H.rolling(lookback, min_periods=lookback).max()
    LLV_now = L.rolling(lookback, min_periods=lookback).min()
    HH_Base = HHV_now.shift(1)
    LL_Base = LLV_now.shift(1)
    base_width = ((HH_Base - LL_Base) / LL_Base) * 100.0
    IsTight = (base_width <= tightLimit).fillna(False)

    rng = H - L
    CloseLoc = pd.Series(np.where(rng > 0, (C - L) / rng, 0.0), index=df.index)

    prevC = C.shift(1)
    IsUpDay = (C > prevC).fillna(False)
    isDownDay = (C < prevC).fillna(False)
    DownVolMax = pd.Series(np.where(isDownDay, V, 0.0), index=df.index).rolling(downLookback, min_periods=downLookback).max()

    MA50 = C.rolling(50, min_periods=50).mean()
    AvgVol20 = V.rolling(20, min_periods=20).mean()

    prevHH_Base = HH_Base.shift(1)
    prevLL_Base = LL_Base.shift(1)
    prevMA50 = MA50.shift(1)

    PocketPivot = (
        IsTight & IsUpDay & V.notna() & DownVolMax.notna() & AvgVol20.notna() &
        (V > DownVolMax) & (V > AvgVol20) & HH_Base.notna() & (C < HH_Base)
    )

    CrossUp = (prevC.notna() & prevHH_Base.notna() & HH_Base.notna() & (prevC <= prevHH_Base) & (C > HH_Base))
    Breakout = (IsTight & CrossUp & V.notna() & AvgVol20.notna() & (V >= AvgVol20 * volBurst) & (CloseLoc > 0.5))
    Buy_Signal = (PocketPivot | Breakout) & AvgVol20.notna() & (AvgVol20 > minVol)

    BaseBreakCrossDown = (IsTight & prevC.notna() & prevLL_Base.notna() & LL_Base.notna() & (prevC >= prevLL_Base) & (C < LL_Base))
    CrossDown_MA50 = (prevC.notna() & prevMA50.notna() & MA50.notna() & (prevC >= prevMA50) & (C < MA50))

    Buy = pd.Series(False, index=df.index)
    Sell = pd.Series(False, index=df.index)
    in_pos = False
    entry = np.nan
    reached10 = False

    for i in range(len(df)):
        c = C.iat[i]
        v = V.iat[i]
        if (not in_pos) and bool(Buy_Signal.iat[i]) and np.isfinite(c) and v20.iat[i] > 1_000_000 and (c >= 10):
            Buy.iat[i] = True
            in_pos = True
            entry = c
            reached10 = False

        if in_pos and np.isfinite(entry) and entry > 0 and np.isfinite(c):
            if c >= entry * 1.1:
                reached10 = True

        stoploss = in_pos and np.isfinite(entry) and entry > 0 and np.isfinite(c) and (c <= entry * 0.9)
        sell_basebreak_vol = in_pos and bool(BaseBreakCrossDown.iat[i]) and np.isfinite(v) and np.isfinite(AvgVol20.iat[i]) and (v > AvgVol20.iat[i] * 1.5)
        sell_ma50_after10 = in_pos and reached10 and bool(CrossDown_MA50.iat[i]) and np.isfinite(v) and np.isfinite(AvgVol20.iat[i]) and (v > AvgVol20.iat[i])

        if stoploss or sell_basebreak_vol or sell_ma50_after10:
            Sell.iat[i] = True
            in_pos = False
            entry = np.nan
            reached10 = False

    out = df[["symbol", "time", "open", "high", "low", "close", "volume"]].copy()
    out["signal"] = np.where(Buy, "BUY", np.where(Sell, "SELL", ""))
    out = out[out["signal"] != ""]
    # latest_date = out["time"].dt.date.max()
    # out = out[out["time"].dt.date == latest_date]
    # out = out[
    #     "symbol open high low close volume signal".split()
    # ]
    # ===== BUILD TRADE LIKE d_trading =====
    if out.empty:
        return pd.DataFrame()

    start_month = (
        pd.Timestamp.now(tz="Asia/Ho_Chi_Minh")
        .normalize()
        .replace(day=1)
        - pd.DateOffset(months=2)
    )

    trades = []
    current_buy = None

    for _, row in out.iterrows():
        if row["signal"] == "BUY":
            current_buy = {
                "symbol": symbol,
                "buy_date": row["time"],
                "buy_price": row["close"],
                "sell_date": None,
                "sell_price": None
            }

        elif row["signal"] == "SELL" and current_buy is not None:
            current_buy["sell_date"] = row["time"]
            current_buy["sell_price"] = row["close"]

            if current_buy["buy_date"] >= start_month:
                trades.append(current_buy)

            current_buy = None

    if current_buy is not None and current_buy["buy_date"] >= start_month:
        trades.append(current_buy)

    if not trades:
        return pd.DataFrame()

    return pd.DataFrame(trades)

# ================== Save to PostgreSQL ==================
def save_pg(symbol):
    symbol = symbol.upper()
    df = pre_break_ignition_signals(symbol)
    if df.empty:
        return f"⚠ Không có dữ liệu Pre-Break {symbol}"

    table = "signals_history.pre_break_ignition_signals"

    start_month = (
        pd.Timestamp.now(tz="Asia/Ho_Chi_Minh")
        .normalize()
        .replace(day=1)
        - pd.DateOffset(months=2)
    )

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        symbol TEXT NOT NULL,
        buy_date TIMESTAMPTZ NOT NULL,
        buy_price DOUBLE PRECISION,
        sell_date TIMESTAMPTZ,
        sell_price DOUBLE PRECISION,
        PRIMARY KEY (symbol, buy_date)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

        conn.execute(
            text("""
                DELETE FROM signals_history.pre_break_ignition_signals
                WHERE symbol = :symbol
                  AND buy_date < :start_month
            """),
            {"symbol": symbol, "start_month": start_month}
        )

        cols = ["symbol", "buy_date", "buy_price", "sell_date", "sell_price"]

        df_insert = df[cols].copy()

        df_insert["sell_date"] = df_insert["sell_date"].astype(object)
        df_insert["sell_price"] = df_insert["sell_price"].astype(object)

        df_insert["sell_date"] = df_insert["sell_date"].where(pd.notna(df_insert["sell_date"]), None)
        df_insert["sell_price"] = df_insert["sell_price"].where(pd.notna(df_insert["sell_price"]), None)

        rows = [tuple(r) for r in df_insert.itertuples(index=False, name=None)]
        
        insert_sql = f"""
        INSERT INTO {table} ("{'","'.join(cols)}")
        VALUES %s
        ON CONFLICT (symbol, buy_date) DO UPDATE SET
            buy_price = EXCLUDED.buy_price,
            sell_date = EXCLUDED.sell_date,
            sell_price = EXCLUDED.sell_price
        """
        execute_values(conn.connection.cursor(), insert_sql, rows, page_size=800)

    return f"✔ {symbol}: {len(df)} rows inserted"

# ================== Multi-symbol ==================
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

    # if len(errors) >= 5:
    #     raise Exception("Task thất bại vì có lỗi:\n" + "\n".join(errors))

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return result