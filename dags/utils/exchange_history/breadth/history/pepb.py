from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    poolclass=NullPool
)

def pepb_breadth():

    asset = pd.read_sql(text("""
        SELECT symbol, exchange FROM info.asset 
        WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
    """), engine)

    pepb_tables = pd.read_sql(text("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'asset_pepb_history'
    """), engine)['table_name'].tolist()

    df_all = pd.concat([
        pd.read_sql(f'SELECT symbol, time, pe, pb FROM asset_pepb_history."{t}"', engine)
        for t in pepb_tables
    ], ignore_index=True)

    df_all['time'] = pd.to_datetime(df_all['time'])
    df_all = df_all.merge(asset, on='symbol', how='inner').sort_values(['symbol', 'time'])

    for exchange in ['HOSE', 'HNX', 'UPCOM']:
        df_ex = df_all[df_all['exchange'] == exchange].copy()

        pe = df_ex.pivot(index='time', columns='symbol', values='pe').sort_index()
        pb = df_ex.pivot(index='time', columns='symbol', values='pb').sort_index()

        den_pe = pe.notna().sum(axis=1).replace(0, float('nan'))
        den_pb = pb.notna().sum(axis=1).replace(0, float('nan'))

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

        result = result.dropna(subset=['peBelowAvg1YPct', 'pbBelowAvg1YPct', 'peBelowAvg3YPct', 'pbBelowAvg3YPct', 'peBelowAvg5YPct', 'pbBelowAvg5YPct'], how='all')

        table = f'breadth_{exchange}'
        with engine.begin() as conn:
            for col in [
                '"peBelowAvg1YPct"', '"pbBelowAvg1YPct"', '"peAbove1Std1YPct"',
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
                        (time, "peBelowAvg1YPct", "pbBelowAvg1YPct", "peAbove1Std1YPct",
                            "peBelowAvg3YPct", "pbBelowAvg3YPct", "peAbove1Std3YPct",
                            "peBelowAvg5YPct", "pbBelowAvg5YPct", "peAbove1Std5YPct")
                    VALUES
                        (:time, :peBelowAvg1YPct, :pbBelowAvg1YPct, :peAbove1Std1YPct,
                                :peBelowAvg3YPct, :pbBelowAvg3YPct, :peAbove1Std3YPct,
                                :peBelowAvg5YPct, :pbBelowAvg5YPct, :peAbove1Std5YPct)
                    ON CONFLICT (time) DO UPDATE SET
                        "peBelowAvg1YPct" = EXCLUDED."peBelowAvg1YPct",
                        "pbBelowAvg1YPct" = EXCLUDED."pbBelowAvg1YPct",
                        "peAbove1Std1YPct" = EXCLUDED."peAbove1Std1YPct",
                        "peBelowAvg3YPct" = EXCLUDED."peBelowAvg3YPct",
                        "pbBelowAvg3YPct" = EXCLUDED."pbBelowAvg3YPct",
                        "peAbove1Std3YPct" = EXCLUDED."peAbove1Std3YPct",
                        "peBelowAvg5YPct" = EXCLUDED."peBelowAvg5YPct",
                        "pbBelowAvg5YPct" = EXCLUDED."pbBelowAvg5YPct",
                        "peAbove1Std5YPct" = EXCLUDED."peAbove1Std5YPct"
                """), row.to_dict())

        print(f"Đã upsert {len(result)} dòng vào exchange_history.{table}")

    print("Hoàn tất!")