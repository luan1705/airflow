from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    poolclass=NullPool
)

def pepb_breadth_update():

    asset = pd.read_sql(text("""
        SELECT symbol, exchange FROM info.asset 
        WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
    """), engine)

    pepb_tables = pd.read_sql(text("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'asset_pepb_history'
    """), engine)['table_name'].tolist()

    # Detect quý mới nhất dùng ACB làm mẫu
    r = pd.read_sql(text("""
        SELECT "calYear", "calQuarter"
        FROM asset_pepb_history."ACB"
        ORDER BY time DESC
        LIMIT 1
    """), engine)

    if r.empty or r.iloc[0]['calYear'] is None:
        print("Không có dữ liệu ACB")
        return

    latest_year    = int(r.iloc[0]['calYear'])
    latest_quarter = int(r.iloc[0]['calQuarter'])

    q_start   = pd.Timestamp(year=latest_year, month=(latest_quarter - 1) * 3 + 1, day=1)
    today     = pd.Timestamp.today().normalize()
    load_from = q_start - pd.Timedelta(days=1560)

    df_all = pd.concat([
        pd.read_sql(f"""
            SELECT symbol, time, pe, pb FROM asset_pepb_history."{t}"
            WHERE time >= '{load_from.date()}'
        """, engine)
        for t in pepb_tables
    ], ignore_index=True)

    df_all['time'] = pd.to_datetime(df_all['time'])
    df_all = df_all.merge(asset, on='symbol', how='inner').sort_values(['symbol', 'time'])

    for exchange in ['HOSE', 'HNX', 'UPCOM']:
        df_ex = df_all[df_all['exchange'] == exchange].copy()

        pe = df_ex.pivot(index='time', columns='symbol', values='pe').sort_index()
        pb = df_ex.pivot(index='time', columns='symbol', values='pb').sort_index()

        WINDOW_1Y = 252
        WINDOW_3Y = 756
        WINDOW_5Y = 1260

        pe_mean_1y = pe.rolling(WINDOW_1Y, min_periods=60).mean()
        pe_std_1y  = pe.rolling(WINDOW_1Y, min_periods=60).std()
        pb_mean_1y = pb.rolling(WINDOW_1Y, min_periods=60).mean()

        pe_mean_3y = pe.rolling(WINDOW_3Y, min_periods=180).mean()
        pe_std_3y  = pe.rolling(WINDOW_3Y, min_periods=180).std()
        pb_mean_3y = pb.rolling(WINDOW_3Y, min_periods=180).mean()

        pe_mean_5y = pe.rolling(WINDOW_5Y, min_periods=252).mean()
        pe_std_5y  = pe.rolling(WINDOW_5Y, min_periods=252).std()
        pb_mean_5y = pb.rolling(WINDOW_5Y, min_periods=252).mean()

        den_pe = pe.notna().sum(axis=1).replace(0, float('nan'))
        den_pb = pb.notna().sum(axis=1).replace(0, float('nan'))

        result = pd.DataFrame({
            'peBelowAvg1YPct':  ((pe < pe_mean_1y).sum(axis=1) / den_pe * 100).round(2),
            'pbBelowAvg1YPct':  ((pb < pb_mean_1y).sum(axis=1) / den_pb * 100).round(2),
            'peAbove1Std1YPct': ((pe > (pe_mean_1y + pe_std_1y)).sum(axis=1) / den_pe * 100).round(2),
            'peBelowAvg3YPct':  ((pe < pe_mean_3y).sum(axis=1) / den_pe * 100).round(2),
            'pbBelowAvg3YPct':  ((pb < pb_mean_3y).sum(axis=1) / den_pb * 100).round(2),
            'peAbove1Std3YPct': ((pe > (pe_mean_3y + pe_std_3y)).sum(axis=1) / den_pe * 100).round(2),
            'peBelowAvg5YPct':  ((pe < pe_mean_5y).sum(axis=1) / den_pe * 100).round(2),
            'pbBelowAvg5YPct':  ((pb < pb_mean_5y).sum(axis=1) / den_pb * 100).round(2),
            'peAbove1Std5YPct': ((pe > (pe_mean_5y + pe_std_5y)).sum(axis=1) / den_pe * 100).round(2),
        }).reset_index()

        result = result[(result['time'] >= q_start) & (result['time'] <= today)]
        result = result.dropna(subset=['peBelowAvg1YPct', 'pbBelowAvg1YPct', 'peBelowAvg3YPct', 'pbBelowAvg3YPct', 'peBelowAvg5YPct', 'pbBelowAvg5YPct'], how='all')

        if result.empty:
            continue

        table = f'breadth_{exchange}'
        with engine.begin() as conn:
            for col in ['"peBelowAvg1YPct"', '"pbBelowAvg1YPct"', '"peAbove1Std1YPct"',
                        '"peBelowAvg3YPct"', '"pbBelowAvg3YPct"', '"peAbove1Std3YPct"',
                        '"peBelowAvg5YPct"', '"pbBelowAvg5YPct"', '"peAbove1Std5YPct"',
                    ]:
                conn.execute(text(f"""
                    ALTER TABLE exchange_history."{table}"
                    ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION
                """))
            for _, row in result.iterrows():
                conn.execute(text(f"""
                    INSERT INTO exchange_history."{table}"
                        (time,
                        "peBelowAvg1YPct", "pbBelowAvg1YPct", "peAbove1Std1YPct",
                        "peBelowAvg3YPct", "pbBelowAvg3YPct", "peAbove1Std3YPct",
                        "peBelowAvg5YPct", "pbBelowAvg5YPct", "peAbove1Std5YPct")
                    VALUES
                        (:time,
                        :peBelowAvg1YPct, :pbBelowAvg1YPct, :peAbove1Std1YPct,
                        :peBelowAvg3YPct, :pbBelowAvg3YPct, :peAbove1Std3YPct,
                        :peBelowAvg5YPct, :pbBelowAvg5YPct, :peAbove1Std5YPct)
                    ON CONFLICT (time) DO UPDATE SET
                        "peBelowAvg1YPct"  = EXCLUDED."peBelowAvg1YPct",
                        "pbBelowAvg1YPct"  = EXCLUDED."pbBelowAvg1YPct",
                        "peAbove1Std1YPct" = EXCLUDED."peAbove1Std1YPct",
                        "peBelowAvg3YPct"  = EXCLUDED."peBelowAvg3YPct",
                        "pbBelowAvg3YPct"  = EXCLUDED."pbBelowAvg3YPct",
                        "peAbove1Std3YPct" = EXCLUDED."peAbove1Std3YPct",
                        "peBelowAvg5YPct"  = EXCLUDED."peBelowAvg5YPct",
                        "pbBelowAvg5YPct"  = EXCLUDED."pbBelowAvg5YPct",
                        "peAbove1Std5YPct" = EXCLUDED."peAbove1Std5YPct"
                """), row.to_dict())

        print(f"✅ {exchange}: {len(result)} dòng")

    print("Hoàn tất!")