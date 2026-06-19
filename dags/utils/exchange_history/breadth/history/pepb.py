from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
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

        WINDOW = 1260

        pe_mean_5y = pe.rolling(WINDOW, min_periods=252).mean()
        pe_std_5y  = pe.rolling(WINDOW, min_periods=252).std()
        pb_mean_5y = pb.rolling(WINDOW, min_periods=252).mean()

        pe_below_avg  = (pe < pe_mean_5y).sum(axis=1) / den_pe * 100
        pb_below_avg  = (pb < pb_mean_5y).sum(axis=1) / den_pb * 100
        pe_above_1std = (pe > (pe_mean_5y + pe_std_5y)).sum(axis=1) / den_pe * 100

        result = pd.DataFrame({
            'peBelowAvg5YPct': pe_below_avg.round(2),
            'pbBelowAvg5YPct': pb_below_avg.round(2),
            'peAbove1StdPct':   pe_above_1std.round(2),
        }).reset_index()

        result = result.dropna(subset=['peBelowAvg5YPct', 'pbBelowAvg5YPct', 'peAbove1StdPct'], how='all')

        table = f'breadth_{exchange}'
        with engine.begin() as conn:
            for col in ['"peBelowAvg5YPct"', '"pbBelowAvg5YPct"', '"peAbove1StdPct"']:
                conn.execute(text(f"""
                    ALTER TABLE exchange_history."{table}"
                    ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION
                """))

            for _, row in result.iterrows():
                conn.execute(text(f"""
                    INSERT INTO exchange_history."{table}"
                        (time, "peBelowAvg5YPct", "pbBelowAvg5YPct", "peAbove1StdPct")
                    VALUES
                        (:time, :peBelowAvg5YPct, :pbBelowAvg5YPct, :peAbove1StdPct)
                    ON CONFLICT (time) DO UPDATE SET
                        "peBelowAvg5YPct" = EXCLUDED."peBelowAvg5YPct",
                        "pbBelowAvg5YPct" = EXCLUDED."pbBelowAvg5YPct",
                        "peAbove1StdPct"   = EXCLUDED."peAbove1StdPct"
                """), row.to_dict())

        print(f"Đã upsert {len(result)} dòng vào exchange_history.{table}")

    print("Hoàn tất!")