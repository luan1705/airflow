import numpy as np
import pandas as pd
# from VNSFintech import *
from datetime import datetime, timedelta
from sqlalchemy import create_engine,text
from psycopg2.extras import execute_values
import concurrent.futures
import logging
import math
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE, indices, custom_list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)

today = datetime.now().date()
batdau=today-timedelta(days=50)

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
)

def d_trading_signals(symbol,swing=5):

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
    
    df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
    df = df.sort_values("time", ascending=True).reset_index(drop=True)

    H = df["high"].astype(float)
    L = df["low"].astype(float)
    C = df["close"].astype(float)
    V = df["volume"].astype(int)

    # 2) Swing TSL
    res = H.rolling(swing, min_periods=swing).max()
    sup = L.rolling(swing, min_periods=swing).min()
    v20 = V.rolling(20).mean()

    prevRes = res.shift(1)
    prevSup = sup.shift(1)

    dirSignal = pd.Series(0, index=df.index, dtype="int64")
    valid = prevRes.notna() & prevSup.notna() & C.notna()
    dirSignal.loc[valid & (C > prevRes)] = 1
    dirSignal.loc[valid & (C < prevSup)] = -1

    lastDir = dirSignal.replace(0, np.nan).ffill().fillna(1).astype(int)

    tsl = pd.Series(np.where(lastDir.values == 1, sup.values, res.values),
                    index=df.index, dtype="float64")

    pivotLine = tsl.shift(1)

    # 3) Cross
    prevClose = C.shift(1)
    prevPivotLine = pivotLine.shift(1)

    buyCross = (prevClose <= prevPivotLine) & (C > pivotLine)
    sellCross = (prevClose >= prevPivotLine) & (C < pivotLine)

    # 4) ExRem/Flip (HOLD)
    Buy = pd.Series(False, index=df.index)
    Sell = pd.Series(False, index=df.index)
    long_state = False

    STOP_LOSS = 0.08  # 8%
    buy_price = None  # track giá mua
    sell_price_actual = pd.Series(np.nan, index=df.index)

    for i in range(len(df)):
        if pd.isna(C.iat[i]) or pd.isna(pivotLine.iat[i]) or pd.isna(prevClose.iat[i]) or pd.isna(prevPivotLine.iat[i]):
            continue
        if buyCross.iat[i] and (not long_state) and (v20.iat[i] > 1_000_000) and (C.iat[i] >= 10):
            Buy.iat[i] = True
            long_state = True
            buy_price = C.iat[i]  # lưu giá mua

        elif long_state and buy_price is not None:
            # Cắt lỗ 8%
            stop_hit = C.iat[i] <= buy_price * (1 - STOP_LOSS)
            # Hoặc TSL signal bình thường
            tsl_hit = sellCross.iat[i] and (C.iat[i] >= 10)

            if stop_hit or tsl_hit:
                Sell.iat[i] = True
                long_state = False
                sell_price_actual.iat[i] = math.ceil(buy_price * (1 - STOP_LOSS) * 100) / 100 if stop_hit else C.iat[i]
                buy_price = None

    
    df["tsl"] = tsl
    df["pivotLine"] = pivotLine
    df["buy"] = Buy
    df["sell"] = Sell
    df["sell_price_actual"] = sell_price_actual
    df = df[df["buy"] | df["sell"]].copy()
    df["signal"] = np.where(df["buy"], "BUY", "SELL")

    # df=df["symbol open close high low volume tsl pivotLine buy sell signal".split()]

    if df.empty:
        return pd.DataFrame()

    last_row = df.iloc[-1]

    # ===== CASE 1: BUY gần nhất =====
    if last_row["buy"]:
        trade = {
            "symbol": symbol,
            "buy_date": last_row["time"],
            "buy_price": last_row["close"],
            "sell_date": None,
            "sell_price": None
        }

    # ===== CASE 2: SELL gần nhất =====
    elif last_row["sell"]:
        # tìm BUY gần nhất phía trước
        prev_buys = df[df["buy"] & (df["time"] < last_row["time"])]

        if prev_buys.empty:
            log.warning(f"⚠ {symbol} SELL nhưng không có BUY trước")
            return pd.DataFrame()

        last_buy = prev_buys.iloc[-1]

        trade = {
            "symbol": symbol,
            "buy_date": last_buy["time"],
            "buy_price": last_buy["close"],
            "sell_date": last_row["time"],
            "sell_price": last_row["sell_price_actual"]
        }

    return pd.DataFrame([trade])

def save_pg(symbol):
    symbol = symbol.upper()
    df = d_trading_signals(symbol)

    if df.empty:
        return f"⚠ Không có dữ liệu trading {symbol}"

    table = 'signals.d_trading_signals'

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
    # result += update_all_symbol(custom_list)
    
    errors = [msg for msg in result if msg.startswith("❌") or msg.startswith("⚠️") or msg.startswith("⚠")]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    # if len(errors) >= 5:
    #     raise Exception("Task thất bại vì có lỗi:\n" + "\n".join(errors))

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return errors if errors else ["Không có lỗi."]