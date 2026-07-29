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

        lnst['isa20'] = pd.to_numeric(lnst['isa20'], errors='coerce')
        lnst['yearReport'] = pd.to_numeric(lnst['yearReport'], errors='coerce')
        lnst['lengthReport'] = pd.to_numeric(lnst['lengthReport'], errors='coerce')

        lnst = lnst.dropna(subset=['isa20', 'yearReport', 'lengthReport'])

        lnst = lnst.sort_values(['yearReport', 'lengthReport'])
        lnst['lnst_ttm'] = lnst['isa20'].rolling(4).sum()
        lnst = lnst.dropna(subset=['lnst_ttm'])
        if lnst.empty:
            return

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

        lnst_ttm = pd.to_numeric(
            lnst.iloc[-1]['lnst_ttm'],
            errors='coerce'
        )

        shares = pd.to_numeric(
            idx.iloc[0]['numberOfSharesMktCap'],
            errors='coerce'
        )

        bsa78 = pd.to_numeric(
            bs.iloc[0]['bsa78'],
            errors='coerce'
        )

        if pd.isna(lnst_ttm) or pd.isna(shares) or pd.isna(bsa78) or shares == 0:
            log.warning(
                f"⚠️ {symbol}: dữ liệu không hợp lệ "
                f"lnst_ttm={lnst_ttm}, shares={shares}, bsa78={bsa78}"
            )
            return

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

        close_val = pd.to_numeric(
            close.iloc[0]['close'],
            errors='coerce'
        )

        if pd.isna(close_val):
            log.warning(f"⚠️ {symbol}: close không hợp lệ")
            return
        date_val  = pd.to_datetime(close.iloc[0]['date']).normalize()

        pe = round((close_val * 1000) / eps_ttm, 2) if eps_ttm else None
        pb = round((close_val * 1000) / bvps,    2) if bvps    else None
        bvps_rounded = round(bvps, 2) if bvps else None

        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO asset_pepb_history."{symbol}"
                    (symbol, time, pe, pb, bvps, "calYear", "calQuarter")
                VALUES
                    (:symbol, :date, :pe, :pb, :bvps, :calYear, :calQuarter)
                ON CONFLICT (time) DO UPDATE SET
                    pe           = EXCLUDED.pe,
                    pb           = EXCLUDED.pb,
                    bvps         = EXCLUDED.bvps,
                    "calYear"    = EXCLUDED."calYear",
                    "calQuarter" = EXCLUDED."calQuarter"
            """), {
                'symbol':     symbol,
                'date':       date_val,
                'pe':         pe,
                'pb':         pb,
                'bvps':       bvps_rounded,
                'calYear':    float(target_year),
                'calQuarter': float(target_quarter),
            })

        log.info(f"✅ {symbol}: pe={pe}, pb={pb}")

    except Exception:
        log.exception(f"❌ {symbol}")


def asset_pepb_history_today():
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')"),
        engine
    )['symbol'].tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(calc_pepb_today, symbols)

    print("Hoàn tất!")