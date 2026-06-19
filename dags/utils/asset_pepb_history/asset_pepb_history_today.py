from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import concurrent.futures
import logging

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
    poolclass=NullPool
)


def calc_pepb_today(symbol: str):
    try:
        lnst = pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", "isa20"
            FROM income_statement."{symbol}"
            WHERE "lengthReport" != 5
            ORDER BY "yearReport", "lengthReport"
        """, engine)

        if lnst.empty:
            return

        lnst = lnst.sort_values(['yearReport', 'lengthReport'])
        lnst['lnst_ttm'] = lnst['isa20'].rolling(4).sum()
        lnst = lnst.dropna(subset=['lnst_ttm'])

        # Lấy quý mới nhất có dữ liệu trong DB
        target_year    = lnst.iloc[-1]['yearReport']
        target_quarter = lnst.iloc[-1]['lengthReport']

        idx = pd.read_sql(f"""
            SELECT "numberOfSharesMktCap"
            FROM index."{symbol}"
            WHERE "ratioType" = 'RATIO_TTM'
              AND "quarter" = {target_quarter}
              AND "yearReport" = {target_year}
            LIMIT 1
        """, engine)

        bs = pd.read_sql(f"""
            SELECT "bsa78"
            FROM balance_sheet."{symbol}"
            WHERE "lengthReport" = {target_quarter}
              AND "yearReport" = {target_year}
            LIMIT 1
        """, engine)

        if idx.empty or bs.empty:
            return

        lnst_ttm = lnst.iloc[-1]['lnst_ttm']
        shares   = idx.iloc[0]['numberOfSharesMktCap']
        bsa78    = bs.iloc[0]['bsa78']

        eps_ttm = lnst_ttm / shares
        bvps    = bsa78    / shares

        close = pd.read_sql(f"""
            SELECT time AT TIME ZONE '+07' AS date, close
            FROM ohlcv."{symbol}_1D"
            WHERE (time AT TIME ZONE '+07')::date = CURRENT_DATE
            LIMIT 1
        """, engine)

        if close.empty:
            return

        close_val = close.iloc[0]['close']
        date_val  = pd.to_datetime(close.iloc[0]['date']).normalize()

        pe = round((close_val * 1000) / eps_ttm, 2) if eps_ttm else None
        pb = round((close_val * 1000) / bvps,    2) if bvps    else None

        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO asset_pepb_history."{symbol}" (symbol, time, pe, pb)
                VALUES (:symbol, :date, :pe, :pb)
                ON CONFLICT (time) DO UPDATE SET
                    pe = EXCLUDED.pe,
                    pb = EXCLUDED.pb
            """), {'symbol': symbol, 'date': date_val, 'pe': pe, 'pb': pb})

        log.info(f"✅ {symbol}: pe={pe}, pb={pb}")

    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


def asset_pepb_history_today():
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')"),
        engine
    )['symbol'].tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(calc_pepb_today, symbols)

    print("Hoàn tất!")