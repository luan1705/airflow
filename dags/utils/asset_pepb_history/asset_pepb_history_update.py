from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import concurrent.futures
import logging
from utils.exchange_history.breadth.update import pepb_breadth_update

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    poolclass=NullPool
)


def calc_pepb_quarter(symbol: str, target_year: float, target_quarter: float):
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

        row_q = lnst[(lnst['yearReport'] == target_year) & (lnst['lengthReport'] == target_quarter)]
        if row_q.empty:
            return

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

        lnst_ttm = float(row_q.iloc[-1]['lnst_ttm'])
        shares   = float(idx.iloc[0]['numberOfSharesMktCap'])
        bsa78    = float(bs.iloc[0]['bsa78'])
        eps_ttm  = lnst_ttm / shares
        bvps     = bsa78 / shares

        # Chỉ lấy close trong khoảng quý đó
        q_start = pd.Timestamp(year=int(target_year), month=int((target_quarter - 1) * 3 + 1), day=1)
        today   = pd.Timestamp.today().normalize()

        close = pd.read_sql(f"""
            SELECT time AT TIME ZONE '+07' AS date, close
            FROM ohlcv."{symbol}_1D"
            WHERE (time AT TIME ZONE '+07')::date BETWEEN :start AND :end
            ORDER BY time
        """, engine, params={'start': q_start, 'end': today})

        if close.empty:
            return

        close['date'] = pd.to_datetime(close['date']).dt.normalize().dt.tz_localize(None)

        with engine.begin() as conn:
            for _, row in close.iterrows():
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
                    'date':       row['date'],
                    'pe':         round((row['close'] * 1000) / eps_ttm, 2) if eps_ttm else None,
                    'pb':         round((row['close'] * 1000) / bvps,    2) if bvps    else None,
                    'bvps':       round(bvps, 2) if bvps else None,
                    'calYear':    float(target_year),
                    'calQuarter': float(target_quarter),
                })

        log.info(f"✅ {symbol}: Q{int(target_quarter)}/{int(target_year)} — {len(close)} dòng")

    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


def _check_and_update(symbol: str, results: list):
    try:
        # Quý mới nhất trong income_statement
        is_latest = pd.read_sql(f"""
            SELECT "yearReport", "lengthReport"
            FROM income_statement."{symbol}"
            WHERE "lengthReport" != 5
            ORDER BY "yearReport" DESC, "lengthReport" DESC
            LIMIT 1
        """, engine)

        if is_latest.empty:
            return

        latest_year    = float(is_latest.iloc[0]['yearReport'])
        latest_quarter = float(is_latest.iloc[0]['lengthReport'])

        # Quý mới nhất đang có trong asset_pepb_history
        try:
            pepb_latest = pd.read_sql(f"""
                SELECT "calYear", "calQuarter"
                FROM asset_pepb_history."{symbol}"
                ORDER BY time DESC
                LIMIT 1
            """, engine)

            if pepb_latest.empty or pepb_latest.iloc[0]['calYear'] is None:
                pepb_year, pepb_quarter = 0.0, 0.0
            else:
                pepb_year    = float(pepb_latest.iloc[0]['calYear'])
                pepb_quarter = float(pepb_latest.iloc[0]['calQuarter'])
        except:
            pepb_year, pepb_quarter = 0.0, 0.0

        if latest_year > pepb_year or (latest_year == pepb_year and latest_quarter > pepb_quarter):
            log.info(f"🔄 {symbol}: BC mới Q{int(latest_quarter)}/{int(latest_year)}")
            calc_pepb_quarter(symbol, latest_year, latest_quarter)
            results.append(symbol)

    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


def asset_pepb_history_update():
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')"),
        engine
    )['symbol'].tolist()
    results = [] 

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda s: _check_and_update(s, results), symbols)

    if results:
        print(f"Có {len(results)} symbol BC mới → chạy pepb_breadth_update")
        pepb_breadth_update()
    else:
        print("Không có BC mới → bỏ qua")

    print("Hoàn tất!")