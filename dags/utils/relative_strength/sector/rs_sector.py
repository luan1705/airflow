import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

SECTOR_WINDOW = 180
BASE = 100.0

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
)

def get_all_sectors():
    """Master list toàn bộ ngành từ info.asset."""
    with engine.begin() as conn:
        sectors = pd.read_sql(
            "SELECT DISTINCT sector FROM info.asset "
            "WHERE sector IS NOT NULL AND sector <> '' "
            "ORDER BY sector",
            conn
        )['sector'].tolist()
    return sectors


def ensure_table_schema(table_name, all_sectors):
    """
    - Nếu chưa có table → CREATE với đủ ngành
    - Nếu có rồi → ADD COLUMN IF NOT EXISTS cho ngành mới (idempotent)
    """
    cols_def = ',\n            '.join(f'"{s}" double precision' for s in all_sectors)
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS ranking.{table_name} (
                date date PRIMARY KEY,
                {cols_def}
            )
        """))
        # Bảo hiểm: ngành mới xuất hiện sau khi table đã tồn tại
        for s in all_sectors:
            conn.execute(text(
                f'ALTER TABLE ranking.{table_name} '
                f'ADD COLUMN IF NOT EXISTS "{s}" double precision'
            ))


def rs_rank_sector(exchange, benchmark):
    all_sectors = get_all_sectors()
    with engine.begin() as conn:
        meta = pd.read_sql(f"""
            SELECT a.symbol, a."sharesOutstanding", a."freeFloatPct", a.sector,
                c."listingDate"
            FROM info.asset a
            JOIN info.company c ON c.symbol = a.symbol
            WHERE a.exchange = '{exchange}'
            AND a.sector IS NOT NULL AND a.sector <> ''
            AND a."sharesOutstanding" IS NOT NULL
            AND a."freeFloatPct" IS NOT NULL
        """, conn)
        meta['weight'] = meta['sharesOutstanding'] * meta['freeFloatPct']
        meta = meta[meta['weight'] > 0].copy()
        meta['listingDate'] = pd.to_datetime(meta['listingDate'])
        meta = meta.set_index('symbol')
        symbols = meta.index.tolist()

    rows = []
    with engine.begin() as conn:
        bm_raw = pd.read_sql(f"""
            SELECT time::date AS date, close FROM ohlcv."{benchmark}"
            ORDER BY time
        """, conn).set_index('date')['close']
        bm_raw.index = pd.to_datetime(bm_raw.index)

        for sym in symbols:
            try:
                df = pd.read_sql(f"""
                    SELECT time::date AS date, close
                    FROM ohlcv."{sym}_1D"
                    ORDER BY time
                """, conn)
                df['symbol'] = sym
                rows.append(df)
            except Exception as e:
                print(f"⚠️ {sym}: {e}")

    prices_raw = pd.concat(rows, ignore_index=True)
    prices_raw['date'] = pd.to_datetime(prices_raw['date'])

    calendar = bm_raw.index
    prices = (prices_raw.pivot(index='date', columns='symbol', values='close')
                        .reindex(calendar)
                        .ffill())

    # Tính sector index: CMV/BMV × 100, base = ngày đầu CMV > 0
    sector_indices = {}
    win = prices.iloc[-(SECTOR_WINDOW + 1):]
    base_date = win.index[0]

    for sector, grp in meta.groupby('sector'):
        eligible = [
            t for t in grp.index
            if t in win.columns and meta.loc[t, 'listingDate'] <= base_date
        ]
        if not eligible:
            continue
        cmv = win[eligible].bfill().mul(meta.loc[eligible, 'weight'], axis=1).sum(axis=1)
        if cmv.iloc[0] == 0:
            continue
        sector_indices[sector] = cmv / cmv.iloc[0] * BASE

    sector_index = pd.DataFrame(sector_indices).sort_index()
    sector_index.index.name = 'date'

    bm = bm_raw.reindex(sector_index.index).ffill()

    def roc(s, n): return (s / s.shift(n) - 1) * 100

    composite = (0.5 * sector_index.apply(lambda s: roc(s, 20) - roc(bm, 20)) +
                 0.3 * sector_index.apply(lambda s: roc(s, 60) - roc(bm, 60)) +
                 0.2 * sector_index.apply(lambda s: roc(s, 120) - roc(bm, 120)))

    rs_pct = composite.rank(axis=1, method='average', pct=True).multiply(100).round(0)
    rs_pct.index.name = 'date'
    rs_pct = rs_pct.reset_index()


    # Bổ sung column ngành chưa có trong rs_pct (exchange này không có stock của ngành đó) → NaN
    for s in all_sectors:
        if s not in rs_pct.columns:
            rs_pct[s] = np.nan
    
    sector_cols = [c for c in rs_pct.columns if c != 'date']
    rs_pct = rs_pct.dropna(how='all', subset=sector_cols).reset_index(drop=True)

    rs_pct = rs_pct[['date'] + all_sectors]

    table_name = f'"rs_sector_{exchange}"'

    ensure_table_schema(table_name, all_sectors)

    # Upsert
    cols = rs_pct.columns.tolist()
    col_list = ', '.join(f'"{c}"' for c in cols)
    # val_placeholders = ', '.join(['%s'] * len(cols))
    update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'date')

    rows_data = [tuple(None if (isinstance(v, float) and np.isnan(v)) else v for v in r)
             for r in rs_pct.itertuples(index=False)]

    
    with engine.begin() as conn:
        with conn.connection.cursor() as cur:
            execute_values(
                cur,
                f"""
                    INSERT INTO ranking.{table_name} ({col_list})
                    VALUES %s
                    ON CONFLICT (date) DO UPDATE SET {update_set}
                """,
                rows_data,
                page_size=1000
            )

    print(f"✅ {exchange}: {len(rs_pct)} dòng đã upsert")


# def save_all_pg():
#     rs_rank_sector('HOSE', 'VNINDEX_1D')
#     rs_rank_sector('HNX', 'HNXINDEX_1D')
#     rs_rank_sector('UPCOM', 'UPCOMINDEX_1D')