from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import concurrent.futures
import logging

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    poolclass=NullPool
)


def calc_pepb(symbol: str):
    try:
        # Lấy LNST từng quý
        lnst = pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", "isa20"
            FROM income_statement."{symbol}"
            WHERE "lengthReport" != 5
            ORDER BY "yearReport", "lengthReport"
        """, engine)

        if lnst.empty:
            log.warning(f"⚠️ {symbol}: không có LNST")
            return

        # Tính LNST TTM = rolling 4 quý
        lnst = lnst.sort_values(['yearReport', 'lengthReport'])
        lnst['lnst_ttm'] = lnst['isa20'].rolling(4).sum()
        lnst = lnst.dropna(subset=['lnst_ttm'])

        # Lấy SLCP theo quý
        idx = pd.read_sql(f"""
            SELECT "yearReport", "quarter" as "lengthReport", "numberOfSharesMktCap"
            FROM index."{symbol}"
            WHERE "ratioType" = 'RATIO_TTM' AND "quarter" != 5
            ORDER BY "yearReport", "quarter"
        """, engine)

        # Lấy vốn chủ theo quý
        bs = pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", "bsa78"
            FROM balance_sheet."{symbol}"
            WHERE "lengthReport" != 5
            ORDER BY "yearReport", "lengthReport"
        """, engine)

        # Merge lnst + slcp + vốn chủ theo quý
        quarterly = lnst.merge(idx, on=['yearReport', 'lengthReport'], how='left') \
                        .merge(bs,  on=['yearReport', 'lengthReport'], how='left')

        # Tính EPS TTM và BVPS
        quarterly['eps_ttm'] = quarterly['lnst_ttm'] / quarterly['numberOfSharesMktCap']
        quarterly['bvps']    = quarterly['bsa78']     / quarterly['numberOfSharesMktCap']

        quarterly = quarterly[['yearReport', 'lengthReport', 'eps_ttm', 'bvps']].dropna()
        quarterly['yearReport']   = quarterly['yearReport'].astype(float)
        quarterly['lengthReport'] = quarterly['lengthReport'].astype(float)

        # Lấy close từng ngày
        close = pd.read_sql(f"""
            SELECT time AT TIME ZONE '+07' AS date, close
            FROM ohlcv."{symbol}_1D"
            ORDER BY time
        """, engine)

        if close.empty:
            log.warning(f"⚠️ {symbol}: không có close")
            return

        close['date'] = pd.to_datetime(close['date']).dt.normalize().dt.tz_localize(None)
        close['yearReport']   = close['date'].dt.year.astype(float)
        close['lengthReport'] = close['date'].dt.quarter.astype(float)

        # Merge theo năm + quý
        df = close.merge(quarterly, on=['yearReport', 'lengthReport'], how='left')

        # Tính PE/PB theo ngày
        df['pe'] = ((df['close']*1000) / df['eps_ttm']).round(2)
        df['pb'] = ((df['close']*1000) / df['bvps']).round(2)
        df['symbol'] = symbol
        df = df[['symbol', 'date', 'pe', 'pb']]

        if df.empty:
            log.warning(f"⚠️ {symbol}: không tính được pe/pb")
            return

        # Upsert vào DB
        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS asset_pepb_history."{symbol}" (
                    symbol  TEXT,
                    time    DATE PRIMARY KEY,
                    pe      DOUBLE PRECISION,
                    pb      DOUBLE PRECISION
                )
            """))
            for _, row in df.iterrows():
                conn.execute(text(f"""
                    INSERT INTO asset_pepb_history."{symbol}" (symbol, time, pe, pb)
                    VALUES (:symbol, :date, :pe, :pb)
                    ON CONFLICT (time) DO UPDATE SET
                        pe = EXCLUDED.pe,
                        pb = EXCLUDED.pb
                """), row.to_dict())

        print(f"✅ {symbol}: {len(df)} dòng")

    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


def asset_pepb_history():
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')"),
        engine
    )['symbol'].tolist()
    # # Test với 5 mã
    # symbols = ['ACB', 'HPG', 'VCB', 'FPT', 'SSI']  

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(calc_pepb, symbols)

    print("Hoàn tất!")