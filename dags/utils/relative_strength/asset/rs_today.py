from sqlalchemy import create_engine
import pandas as pd
import concurrent.futures
import logging
from psycopg2.extras import execute_values
from utils.create_list.indices_map import indices_map
from datetime import datetime
import pytz

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
    pool_size=10, max_overflow=20, pool_timeout=60
)

PRIORITY = ['VNINDEX', 'HNXINDEX', 'UPCOMINDEX']

symbol_exchange = {}
for exchange in PRIORITY:
    for symbol in indices_map.get(exchange, []):
        if symbol not in symbol_exchange:
            symbol_exchange[symbol] = exchange


def get_today():
    return datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).date()


def calc_rs_today(symbol: str, today) -> str:
    try:
        exchange = symbol_exchange.get(symbol)
        if not exchange:
            return f"⚠️ Không tìm thấy exchange cho {symbol}"

        # Chỉ đọc 121 dòng gần nhất — đủ để tính shift(120)
        df = pd.read_sql(f"""
            SELECT * FROM (
                SELECT s.time, s.symbol, s.open, s.close,
                       e.open AS e_open, e.close AS e_close
                FROM ohlcv."{symbol}_1D" s
                JOIN ohlcv."{exchange}_1D" e ON e.time = s.time
                ORDER BY s.time DESC
                LIMIT 121
            ) t ORDER BY time ASC
        """, engine)

        if df.empty:
            return f"⚠️ Không có dữ liệu cho {symbol}"

        df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")

        for n, label in [(20, "1m"), (60, "3m"), (120, "6m")]:
            open_n   = df["open"].shift(n)
            e_open_n = df["e_open"].shift(n)
            df[f"rs_{label}"] = (
                (df["close"] - open_n)     / open_n   * 100 -
                (df["e_close"] - e_open_n) / e_open_n * 100
            )

        df["rs"] = df["rs_1m"] * 0.5 + df["rs_3m"] * 0.3 + df["rs_6m"] * 0.2
        df = df[["time", "rs"]].dropna(subset=["rs"])

        # Chỉ lấy dòng hôm nay
        df = df[df["time"].dt.date == today]
        if df.empty:
            return f"⚠️ Chưa có dữ liệu hôm nay ({today}) cho {symbol}"

        rows = [(symbol, row.time, row.rs) for row in df.itertuples()]
        with engine.begin() as conn:
            conn.exec_driver_sql(f'''
                CREATE TABLE IF NOT EXISTS indicator."{symbol}_1D" (
                    symbol   text        NOT NULL,
                    time     timestamptz PRIMARY KEY,
                    rs       double precision,
                    "rsRank"  double precision
                )
            ''')
            with conn.connection.cursor() as cur:
                execute_values(
                    cur,
                    f'''
                        INSERT INTO indicator."{symbol}_1D" (symbol, time, rs)
                        VALUES %s
                        ON CONFLICT (time) DO UPDATE
                            SET rs = EXCLUDED.rs
                    ''',
                    rows,
                    template="(%s::text, %s::timestamptz, %s::double precision)",
                    page_size=1000
                )

        return f"✅ {symbol}"

    except Exception as e:
        msg = f"❌ {symbol}: {e}"
        log.error(msg)
        return msg


def rs_rank_today(today):
    log.info(f"🚀 Tính rs_rank ngày {today}...")

    today_rs = []
    for symbol, exchange in symbol_exchange.items():
        try:
            df = pd.read_sql(f"""
                SELECT symbol, time, rs
                FROM indicator."{symbol}_1D"
                WHERE (time AT TIME ZONE 'Asia/Ho_Chi_Minh')::date = '{today}'
                AND rs IS NOT NULL
            """, engine)
            if df.empty:
                continue
            df["exchange"] = exchange
            df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
            today_rs.append(df)
        except Exception as e:
            log.warning(f"⚠️ {symbol}: {e}")

    if not today_rs:
        log.warning("⚠️ Không có dữ liệu rs hôm nay.")
        return ["⚠️ Không có dữ liệu."]

    df_all = pd.concat(today_rs, ignore_index=True)
    df_all["rsRank"] = (
        df_all.groupby(["time", "exchange"])["rs"]
        .rank(pct=True) * 100
    ).round(0)

    errors = []
    for symbol, grp in df_all.groupby("symbol"):
        try:
            rows = [(row.rsRank, row.time) for row in grp.itertuples()]
            with engine.begin() as conn:
                with conn.connection.cursor() as cur:
                    execute_values(
                        cur,
                        f"""
                            UPDATE indicator."{symbol}_1D"
                            SET "rsRank" = data.rsRank
                            FROM (VALUES %s) AS data(rsRank, time)
                            WHERE "{symbol}_1D".time = data.time
                        """,
                        rows,
                        template="(%s::double precision, %s::timestamptz)",
                        page_size=1000
                    )
        except Exception as e:
            msg = f"❌ {symbol}: {e}"
            log.error(msg)
            errors.append(msg)

    log.info(f"🎉 Hoàn thành rsRank ngày {today}.")
    return errors if errors else [f"✅ rs_rank ngày {today}"]


def etl_rs_rs_rank_today():
    today = get_today()
    log.info(f"🚀 Bắt đầu ETL realtime ngày {today}...")

    stock_symbols = list(symbol_exchange.keys())

    # Bước 1: Tính rs hôm nay song song
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda s: calc_rs_today(s, today), stock_symbols))

    errors = [m for m in results if m.startswith("❌") or m.startswith("⚠️")]
    log.info(f"✅ rs: {len(results) - len(errors)} mã | lỗi: {len(errors)}")

    # Bước 2: Tính rs_rank hôm nay
    rank_errors = rs_rank_today(today)

    all_errors = errors + [e for e in rank_errors if not e.startswith("✅")]
    return all_errors if all_errors else [f"✅ Hoàn thành ETL realtime ngày {today}"]