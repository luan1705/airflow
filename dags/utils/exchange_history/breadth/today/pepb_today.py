from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
    poolclass=NullPool
)

def pepb_breadth_today():

    today = pd.Timestamp.today().normalize()

    asset = pd.read_sql(text("""
        SELECT symbol, exchange FROM info.asset 
        WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
    """), engine)

    pepb_tables = pd.read_sql(text("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'asset_pepb_history'
    """), engine)['table_name'].tolist()

    df_all = pd.concat([
        pd.read_sql(f"SELECT symbol, time, pe, pb FROM asset_pepb_history.\"{t}\" WHERE time >= CURRENT_DATE - INTERVAL '1560 days'", engine)
        for t in pepb_tables
    ], ignore_index=True)

    df_all['time'] = pd.to_datetime(df_all['time'])
    df_all = df_all.merge(asset, on='symbol', how='inner').sort_values(['symbol', 'time'])

    for exchange in ['HOSE', 'HNX', 'UPCOM']:
        df_ex = df_all[df_all['exchange'] == exchange].copy()

        pe = df_ex.pivot(index='time', columns='symbol', values='pe').sort_index()
        pb = df_ex.pivot(index='time', columns='symbol', values='pb').sort_index()

        WINDOW = 1260
        pe_tail    = pe.tail(WINDOW)
        pb_tail    = pb.tail(WINDOW)
        pe_mean_5y = pe_tail.mean()
        pe_std_5y  = pe_tail.std()
        pb_mean_5y = pb_tail.mean()

        pe_today = pe.iloc[-1]
        pb_today = pb.iloc[-1]

        valid_pe     = pe_today.notna() & pe_mean_5y.notna()
        valid_pb     = pb_today.notna() & pb_mean_5y.notna()
        valid_pe_std = valid_pe & pe_std_5y.notna()

        if not valid_pe.any():
            print(f"Không có dữ liệu hôm nay cho {exchange}")
            continue

        row = {
            'time':            today,
            'peBelowAvg5YPct': round((pe_today[valid_pe] < pe_mean_5y[valid_pe]).sum() / valid_pe.sum() * 100, 2),
            'pbBelowAvg5YPct': round((pb_today[valid_pb] < pb_mean_5y[valid_pb]).sum() / valid_pb.sum() * 100, 2),
            'peAbove1StdPct':  round((pe_today[valid_pe_std] > (pe_mean_5y + pe_std_5y)[valid_pe_std]).sum() / valid_pe_std.sum() * 100, 2),
        }

        table = f'breadth_{exchange}'
        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO exchange_history."{table}"
                    (time, "peBelowAvg5YPct", "pbBelowAvg5YPct", "peAbove1StdPct")
                VALUES
                    (:time, :peBelowAvg5YPct, :pbBelowAvg5YPct, :peAbove1StdPct)
                ON CONFLICT (time) DO UPDATE SET
                    "peBelowAvg5YPct" = EXCLUDED."peBelowAvg5YPct",
                    "pbBelowAvg5YPct" = EXCLUDED."pbBelowAvg5YPct",
                    "peAbove1StdPct"  = EXCLUDED."peAbove1StdPct"
            """), row)

        print(f"✅ {exchange}: {row}")

    print("Hoàn tất!")