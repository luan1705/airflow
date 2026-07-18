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
                sell_price_actual.iat[i] = math.ceil(buy_price * (1 - STOP_LOSS) * 100) / 100 if stop_hit else C.iat[i]  # ✅
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

    # lấy các lệnh có buy_date trong 3 tháng gần nhất
    start_month = (
    pd.Timestamp.now(tz="Asia/Ho_Chi_Minh")
    .normalize()
    .replace(day=1)
    - pd.DateOffset(months=2)
)

    trades = []
    current_buy = None

    for _, row in df.iterrows():
        if row["buy"]:
            current_buy = {
                "symbol": symbol,
                "buy_date": row["time"],
                "buy_price": row["close"],
                "sell_date": None,
                "sell_price": None
            }

        elif row["sell"] and current_buy is not None:
            current_buy["sell_date"] = row["time"]
            current_buy["sell_price"] = row["sell_price_actual"] 

            if current_buy["buy_date"] >= start_month:
                trades.append(current_buy)

            current_buy = None

    # nếu còn BUY đang mở, cũng lưu
    if current_buy is not None and current_buy["buy_date"] >= start_month:
        trades.append(current_buy)

    if not trades:
        return pd.DataFrame()

    return pd.DataFrame(trades)

def save_pg(symbol):
    symbol = symbol.upper()
    df = d_trading_signals(symbol)

    if df.empty:
        return f"⚠ Không có dữ liệu trading {symbol}"

    table = 'signals_history.d_trading_signals'

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
        conn.execute(text("""
                        DELETE FROM signals_history.d_trading_signals
                        WHERE symbol = :symbol
                        AND buy_date < :start_month
                    """),
                    {"symbol": symbol, "start_month": start_month}
                )

        cols = ["symbol", "buy_date", "buy_price", "sell_date", "sell_price"]

        df_insert = df[cols].copy()

        # ép NaT/NaN thành None để PostgreSQL lưu thành NULL
        df_insert["sell_date"] = df_insert["sell_date"].astype(object)
        df_insert["sell_price"] = df_insert["sell_price"].astype(object)

        df_insert["sell_date"] = df_insert["sell_date"].where(pd.notna(df_insert["sell_date"]), None)
        df_insert["sell_price"] = df_insert["sell_price"].where(pd.notna(df_insert["sell_price"]), None)

        rows = [tuple(r) for r in df_insert.itertuples(index=False, name=None)]

        insert_sql = f"""
        INSERT INTO {table} ("{'","'.join(cols)}")
        VALUES %s
        ON CONFLICT (symbol,buy_date) DO UPDATE SET
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

if __name__ == "__main__":
    save_all_pg()