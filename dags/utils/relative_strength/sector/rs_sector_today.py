import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.sql.expression import quoted_name
from psycopg2.extras import execute_values
from datetime import date, timedelta

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
)

def rs_rank_sector_today(exchange, benchmark):
    today = date.today()
    lookback = (today - timedelta(days=200)).strftime('%Y-%m-%d')
    base_date = '2024-01-01'

    with engine.begin() as conn:
        symbols = pd.read_sql(
            f"SELECT symbol FROM info.asset WHERE exchange = '{exchange}'", conn
        )['symbol'].tolist()

    rows_base = []
    rows_recent = []
    with engine.begin() as conn:
        for sym in symbols:
            try:
                df_base = pd.read_sql(f"""
                    SELECT o.symbol, o.time::date as date, o.close,
                           i."sharesOutstanding", i."freeFloatPct", i."sector"
                    FROM ohlcv."{sym}_1D" o
                    JOIN info."asset" i ON i.symbol = '{sym}'
                    WHERE o.time::date = '{base_date}'
                """, conn)
                rows_base.append(df_base)

                df_recent = pd.read_sql(f"""
                    SELECT o.symbol, o.time::date as date, o.close,
                           i."sharesOutstanding", i."freeFloatPct", i."sector"
                    FROM ohlcv."{sym}_1D" o
                    JOIN info."asset" i ON i.symbol = '{sym}'
                    WHERE o.time >= '{lookback}'
                """, conn)
                rows_recent.append(df_recent)
            except:
                pass

    rows_base = [df for df in rows_base if not df.empty]
    rows_recent = [df for df in rows_recent if not df.empty]
    if not rows_base or not rows_recent:
        print(f"⚠️ {exchange}: không có dữ liệu")
        return

    df_base = pd.concat(rows_base, ignore_index=True)
    df_base['weight'] = df_base['sharesOutstanding'] * df_base['freeFloatPct']
    df_base['mcap'] = df_base['close'] * df_base['weight']
    bmv = df_base.groupby('sector')['mcap'].sum().rename('bmv')

    df = pd.concat(rows_recent, ignore_index=True)
    df['weight'] = df['sharesOutstanding'] * df['freeFloatPct']
    df['mcap'] = df['close'] * df['weight']

    cmv = df.groupby(['date', 'sector'])['mcap'].sum().reset_index()
    cmv.columns = ['date', 'sector', 'cmv']
    cmv = cmv.join(bmv, on='sector')
    cmv['index'] = (cmv['cmv'] / cmv['bmv']) * 100
    sector_index = cmv.pivot(index='date', columns='sector', values='index')

    with engine.begin() as conn:
        bm = pd.read_sql(f"""
            SELECT time::date as date, close FROM ohlcv."{benchmark}"
            WHERE time >= '{lookback}' ORDER BY time
        """, conn).set_index('date')['close']

    bm = bm.reindex(sector_index.index).ffill()

    def roc(s, n): return (s / s.shift(n) - 1) * 100

    composite = (0.3 * sector_index.apply(lambda s: roc(s, 20) - roc(bm, 20)) +
                 0.5 * sector_index.apply(lambda s: roc(s, 60) - roc(bm, 60)) +
                 0.2 * sector_index.apply(lambda s: roc(s, 120) - roc(bm, 120)))

    rs_pct = composite.rank(axis=1, method='average', pct=True).multiply(100).round(0)
    rs_pct.index.name = 'date'
    rs_pct = rs_pct.reset_index()

    today_row = rs_pct[rs_pct['date'] == today]
    if today_row.empty:
        print(f"⚠️ {exchange}: chưa có dữ liệu ngày {today}")
        return

    table_name = f'"rs_sector_{exchange}"'
    cols = today_row.columns.tolist()
    col_list = ', '.join(f'"{c}"' for c in cols)
    update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'date')
    rows_data = [tuple(r) for r in today_row.itertuples(index=False)]

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

    print(f"✅ {exchange}: đã upsert ngày {today}")


# def update_all():
#     rs_rank_sector_today('HOSE', 'VNINDEX_1D')
#     rs_rank_sector_today('HNX', 'HNXINDEX_1D')
#     rs_rank_sector_today('UPCOM', 'UPCOMINDEX_1D')
