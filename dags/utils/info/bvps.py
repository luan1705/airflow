from sqlalchemy import create_engine, text
import pandas as pd
import concurrent.futures
import logging
from sqlalchemy.pool import NullPool
from db_config import POSTGRES_URL

log = logging.getLogger(__name__)

engine = create_engine(
    POSTGRES_URL,
    poolclass=NullPool,
    pool_pre_ping=True
)

def update_bvps(symbol):
    try:
        df = pd.read_sql(f"""
            SELECT bvps FROM asset_pepb_history."{symbol}"
            ORDER BY time DESC
            LIMIT 1
        """, engine)
        if df.empty or pd.isna(df.iloc[0]['bvps']):
            return
        bvps = float(df.iloc[0]['bvps'])
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE info.asset SET bvps = :bvps WHERE symbol = :symbol
            """), {'bvps': bvps, 'symbol': symbol})
        print(f"✅ {symbol}: bvps={bvps}")
    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


def update_all_bvps():
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE info.asset ADD COLUMN IF NOT EXISTS bvps DOUBLE PRECISION"))

    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')"),
        engine
    )['symbol'].tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(update_bvps, symbols)

    print("Hoàn tất!")

# if __name__ == '__main__':
#     update_all_bvps()