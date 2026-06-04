import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.sql.expression import quoted_name

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
)

def rs_rank_sector(exchange, benchmark):
    with engine.begin() as conn:
        symbols = pd.read_sql(
            f"SELECT symbol FROM info.asset WHERE exchange = '{exchange}'", conn
        )['symbol'].tolist()

    rows = []
    with engine.begin() as conn:
        for sym in symbols:
            try:
                df = pd.read_sql(f"""
                    SELECT o.symbol, o.time::date as date, o.close,
                           i."sharesOutstanding", i."freeFloatPct", i."sector"
                    FROM ohlcv."{sym}_1D" o
                    JOIN info."asset" i ON i.symbol = '{sym}'
                    WHERE o.time >= '2024-01-01'
                """, conn)
                rows.append(df)
            except:
                pass

    rows = [df for df in rows if not df.empty]
    df = pd.concat(rows, ignore_index=True)

    df['weight'] = df['sharesOutstanding'] * df['freeFloatPct']
    df['mcap'] = df['close'] * df['weight']

    cmv = df.groupby(['date', 'sector'])['mcap'].sum().reset_index()
    cmv.columns = ['date', 'sector', 'cmv']
    bmv = cmv.groupby('sector')['cmv'].first().rename('bmv')
    cmv = cmv.join(bmv, on='sector')
    cmv['index'] = (cmv['cmv'] / cmv['bmv']) * 100
    sector_index = cmv.pivot(index='date', columns='sector', values='index')

    with engine.begin() as conn:
        bm = pd.read_sql(f"""
            SELECT time::date as date, close FROM ohlcv."{benchmark}"
            WHERE time >= '2024-01-01' ORDER BY time
        """, conn).set_index('date')['close']

    bm = bm.reindex(sector_index.index).ffill()

    def roc(s, n): return (s / s.shift(n) - 1) * 100

    composite = (0.3 * sector_index.apply(lambda s: roc(s, 20) - roc(bm, 20)) +
                 0.5 * sector_index.apply(lambda s: roc(s, 60) - roc(bm, 60)) +
                 0.2 * sector_index.apply(lambda s: roc(s, 120) - roc(bm, 120)))

    rs_pct = composite.rank(axis=1, method='average', pct=True).multiply(100).round(0)
    rs_pct.index.name = 'date'
    rs_pct = rs_pct.reset_index()
    rs_pct = rs_pct.iloc[120:].reset_index(drop=True)

    table_name = f'"rs_sector_{exchange}"'

    # Tạo bảng nếu chưa có
    cols_def = ', '.join(f'"{c}" double precision' for c in rs_pct.columns if c != 'date')
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS ranking.{table_name} (
                date date PRIMARY KEY,
                {cols_def}
            )
        """))

    # Upsert
    cols = rs_pct.columns.tolist()
    col_list = ', '.join(f'"{c}"' for c in cols)
    val_placeholders = ', '.join(['%s'] * len(cols))
    update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'date')

    rows_data = [tuple(r) for r in rs_pct.itertuples(index=False)]

    from psycopg2.extras import execute_values
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