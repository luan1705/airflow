import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
from datetime import date, timedelta

WINDOW = 180
BASE = 100.0
engine = create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech")

def rs_rank_sector_today(exchange, benchmark, n_upsert=1):
    # 1) meta: LỌC GIỐNG HỆT bản full
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

    # 2) đọc DƯ phiên để roc120 ở dòng cuối còn đủ neo (~400 ngày lịch ≈ 270 phiên)
    lookback = (date.today() - timedelta(days=400)).strftime('%Y-%m-%d')
    with engine.begin() as conn:
        bm_raw = pd.read_sql(f"""
            SELECT time::date AS date, close FROM ohlcv."{benchmark}"
            WHERE time >= '{lookback}' ORDER BY time
        """, conn).set_index('date')['close']
        bm_raw.index = pd.to_datetime(bm_raw.index)

        rows = []
        for sym in symbols:
            try:
                df = pd.read_sql(f"""
                    SELECT time::date AS date, close
                    FROM ohlcv."{sym}_1D"
                    WHERE time >= '{lookback}' ORDER BY time
                """, conn)
                df['symbol'] = sym
                rows.append(df)
            except Exception as e:
                print(f"⚠️ {sym}: {e}")

    prices_raw = pd.concat(rows, ignore_index=True)
    prices_raw['date'] = pd.to_datetime(prices_raw['date'])

    # 3) pivot + reindex theo lịch benchmark + ffill TỪNG MÃ (mấu chốt)
    calendar = bm_raw.index
    prices = (prices_raw.pivot(index='date', columns='symbol', values='close')
                        .reindex(calendar).ffill())

    win = prices.iloc[-(WINDOW + 1):]
    base_date = win.index[0]

    # 4) sector index với rổ CỐ ĐỊNH (eligible: listingDate <= base_date)
    sector_indices = {}
    for sector, grp in meta.groupby('sector'):
        eligible = [t for t in grp.index
                    if t in win.columns and meta.loc[t, 'listingDate'] <= base_date]
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

    # 5) chỉ ghi n dòng cuối (mặc định = phiên mới nhất)
    rs_pct = rs_pct.tail(n_upsert)

    table_name = f'"rs_sector_{exchange}"'
    cols = rs_pct.columns.tolist()
    col_list = ', '.join(f'"{c}"' for c in cols)
    update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'date')
    rows_data = [tuple(None if (isinstance(v, float) and np.isnan(v)) else v for v in r)
                 for r in rs_pct.itertuples(index=False)]

    with engine.begin() as conn:
        with conn.connection.cursor() as cur:
            execute_values(cur, f"""
                INSERT INTO ranking.{table_name} ({col_list})
                VALUES %s
                ON CONFLICT (date) DO UPDATE SET {update_set}
            """, rows_data, page_size=1000)

    print(f"✅ {exchange}: upsert {len(rs_pct)} dòng (đến {rs_pct['date'].max().date()})")