from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
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
        pd.read_sql(f"SELECT symbol, time, pe, pb FROM asset_pepb_history.\"{t}\" WHERE time >= CURRENT_DATE - INTERVAL '7 years'", engine)
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

        pe_hist = pe.shift(1)
        pb_hist = pb.shift(1)

        pe_mean_1y = pe_hist.rolling(WINDOW_1Y, min_periods=60).mean()
        pe_std_1y  = pe_hist.rolling(WINDOW_1Y, min_periods=60).std()
        pb_mean_1y = pb_hist.rolling(WINDOW_1Y, min_periods=60).mean()

        pe_mean_3y = pe_hist.rolling(WINDOW_3Y, min_periods=180).mean()
        pe_std_3y  = pe_hist.rolling(WINDOW_3Y, min_periods=180).std()
        pb_mean_3y = pb_hist.rolling(WINDOW_3Y, min_periods=180).mean()

        pe_mean_5y = pe_hist.rolling(WINDOW_5Y, min_periods=252).mean()
        pe_std_5y  = pe_hist.rolling(WINDOW_5Y, min_periods=252).std()
        pb_mean_5y = pb_hist.rolling(WINDOW_5Y, min_periods=252).mean()

        pe_today = pe.iloc[-1]
        pb_today = pb.iloc[-1]

        pe_mean_1y_today = pe_mean_1y.iloc[-1]
        pe_std_1y_today  = pe_std_1y.iloc[-1]
        pb_mean_1y_today = pb_mean_1y.iloc[-1]

        pe_mean_3y_today = pe_mean_3y.iloc[-1]
        pe_std_3y_today  = pe_std_3y.iloc[-1]
        pb_mean_3y_today = pb_mean_3y.iloc[-1]

        pe_mean_5y_today = pe_mean_5y.iloc[-1]
        pe_std_5y_today  = pe_std_5y.iloc[-1]
        pb_mean_5y_today = pb_mean_5y.iloc[-1]

        valid_pe_1y = pe_today.notna() & pe_mean_1y_today.notna()
        valid_pe_3y = pe_today.notna() & pe_mean_3y_today.notna()
        valid_pe_5y = pe_today.notna() & pe_mean_5y_today.notna()

        valid_pb_1y = pb_today.notna() & pb_mean_1y_today.notna()
        valid_pb_3y = pb_today.notna() & pb_mean_3y_today.notna()
        valid_pb_5y = pb_today.notna() & pb_mean_5y_today.notna()


        if not (
            valid_pe_1y.any() or valid_pe_3y.any() or valid_pe_5y.any() or valid_pb_1y.any() or valid_pb_3y.any() or valid_pb_5y.any()
        ):
            print(f"Không có dữ liệu hôm nay cho {exchange}")
            continue

        def pct(condition, valid):
            return round(condition[valid].sum() / valid.sum() * 100, 2) if valid.any() else None


        row = {
            'time': today,
            'peBelowAvg1YPct': pct(pe_today < pe_mean_1y_today, valid_pe_1y),
            'pbBelowAvg1YPct': pct(pb_today < pb_mean_1y_today, valid_pb_1y),
            'peAbove1Std1YPct': pct(pe_today > (pe_mean_1y_today + pe_std_1y_today), valid_pe_1y & pe_std_1y_today.notna()),

            'peBelowAvg3YPct': pct(pe_today < pe_mean_3y_today, valid_pe_3y),
            'pbBelowAvg3YPct': pct(pb_today < pb_mean_3y_today, valid_pb_3y),
            'peAbove1Std3YPct': pct(pe_today > (pe_mean_3y_today + pe_std_3y_today), valid_pe_3y & pe_std_3y_today.notna()),

            'peBelowAvg5YPct': pct(pe_today < pe_mean_5y_today, valid_pe_5y),
            'pbBelowAvg5YPct': pct(pb_today < pb_mean_5y_today, valid_pb_5y),
            'peAbove1Std5YPct': pct(pe_today > (pe_mean_5y_today + pe_std_5y_today), valid_pe_5y & pe_std_5y_today.notna()),
        }

        table = f'breadth_{exchange}'
        with engine.begin() as conn:
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
            """), row)

        print(f"✅ {exchange}: {row}")

    print("Hoàn tất!")
