from sqlalchemy import create_engine, text
import pandas as pd
import concurrent.futures
import logging
from psycopg2.extras import execute_values
from utils.create_list.indices_map import indices_map

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


def rs_rank():
    log.info("🚀 Bắt đầu tính rs_rank...")

    all_rs = []
    for symbol, exchange in symbol_exchange.items():
        try:
            df = pd.read_sql(f"""
                SELECT symbol, time, rs
                FROM indicator."{symbol}_1D"
                WHERE rs IS NOT NULL
                ORDER BY time ASC
            """, engine)

            if df.empty:
                continue

            df["exchange"] = exchange
            df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
            all_rs.append(df)

        except Exception as e:
            log.warning(f"⚠️ {symbol}: {e}")

    if not all_rs:
        log.warning("⚠️ Không có dữ liệu rs nào để tính rank.")
        return ["⚠️ Không có dữ liệu."]
    
    all_rs = pd.concat(all_rs, ignore_index=True)

    all_rs["rsRank"] = (
        all_rs.groupby(["time", "exchange"])["rs"]
        .rank(pct=True) * 100
    ).round(0)

    errors = []
    for symbol, grp in all_rs.groupby("symbol"):
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
            log.info(f"✅ {symbol}: {len(rows)} dòng")
        except Exception as e:
            msg = f"❌ {symbol}: {e}"
            log.error(msg)
            errors.append(msg)

    log.info("🎉 Hoàn thành ETL rs_rank.")
    return errors if errors else ["✅ Tất cả mã đã được xử lý thành công!"]