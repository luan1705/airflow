from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
    poolclass=NullPool
)

def adv_dec_indicators():

    asset = pd.read_sql(text("SELECT symbol, exchange FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')"), engine)
    tables = pd.read_sql(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ohlcv' AND table_name LIKE '%_1D'"), engine)['table_name'].tolist()

    df_all = pd.concat([
        pd.read_sql(f'SELECT time, close FROM ohlcv."{t}"', engine).assign(symbol=t.replace('_1D', ''))
        for t in tables
    ], ignore_index=True)

    df_all['time'] = pd.to_datetime(df_all['time'], utc=True).dt.normalize().dt.tz_localize(None)
    df_all = df_all[df_all['time'].dt.dayofweek < 5].merge(asset, on='symbol', how='inner').sort_values(['symbol', 'time'])

    for exchange in ['HOSE', 'HNX', 'UPCOM']:
        df_ex = df_all[df_all['exchange'] == exchange].copy()
        close = df_ex.pivot(index='time', columns='symbol', values='close').sort_index()

        # Adv/Dec/NoChange
        df_ex['change'] = df_ex.groupby('symbol')['close'].pct_change(fill_method=None)
        df_ex['status'] = df_ex['change'].apply(lambda x: None if pd.isna(x) else ('advancers' if x > 0 else ('decliners' if x < 0 else 'noChanges')))
        adn = df_ex.dropna(subset=['status']).groupby(['time', 'status']).size().unstack(fill_value=0).reset_index()
        for col in ['advancers', 'decliners', 'noChanges']:
            if col not in adn.columns:
                adn[col] = 0
        total = adn[['advancers', 'decliners', 'noChanges']].sum(axis=1)
        adn['advancersPct'] = (adn['advancers'] / total * 100).round(2)
        adn['noChangesPct'] = (adn['noChanges'] / total * 100).round(2)
        adn['declinersPct'] = (adn['decliners'] / total * 100).round(2)

        # Indicators
        den   = close.notna().sum(axis=1).replace(0, float('nan'))
        delta = close.diff()
        gain  = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
        loss  = (-delta.clip(upper=0)).ewm(span=14, adjust=False).mean()
        rsi   = 100 - (100 / (1 + gain / loss))
        macd  = close.ewm(span=13, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
        high_52w = close.shift(1).rolling(252).max()
        low_52w  = close.shift(1).rolling(252).min()

        ind = pd.DataFrame({
            'aboveEma20Pct':   ((close > close.ewm(span=20,  adjust=False).mean()).sum(axis=1) / den * 100).round(2),
            'aboveEma50Pct':   ((close > close.ewm(span=50,  adjust=False).mean()).sum(axis=1) / den * 100).round(2),
            'aboveEma100Pct':  ((close > close.ewm(span=100, adjust=False).mean()).sum(axis=1) / den * 100).round(2),
            'aboveEma200Pct':  ((close > close.ewm(span=200, adjust=False).mean()).sum(axis=1) / den * 100).round(2),
            'aboveRsi50Pct':   ((rsi > 50).sum(axis=1) / den * 100).round(2),
            'aboveRsi70Pct':   ((rsi > 70).sum(axis=1) / den * 100).round(2),
            'belowRsi30Pct':   ((rsi < 30).sum(axis=1) / den * 100).round(2),
            'positiveMacdPct': ((macd > 0).sum(axis=1) / den * 100).round(2),
            'above52WHighPct': ((close > high_52w).sum(axis=1) / den * 100).round(2),
            'below52WLowPct':  ((close < low_52w).sum(axis=1)  / den * 100).round(2),
        }, index=close.index).reset_index()

        final = adn.merge(ind, on='time', how='inner')
        final = final.drop(columns=['advancers', 'decliners', 'noChanges'])

        # Upsert
        table = f'breadth_{exchange}'
        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS exchange_history."{table}" (
                    time              DATE PRIMARY KEY,
                    "advancersPct"    DOUBLE PRECISION,
                    "noChangesPct"    DOUBLE PRECISION,
                    "declinersPct"    DOUBLE PRECISION,
                    "aboveEma20Pct"   DOUBLE PRECISION,
                    "aboveEma50Pct"   DOUBLE PRECISION,
                    "aboveEma100Pct"  DOUBLE PRECISION,
                    "aboveEma200Pct"  DOUBLE PRECISION,
                    "aboveRsi50Pct"   DOUBLE PRECISION,
                    "aboveRsi70Pct"   DOUBLE PRECISION,
                    "belowRsi30Pct"   DOUBLE PRECISION,
                    "positiveMacdPct" DOUBLE PRECISION,
                    "above52WHighPct" DOUBLE PRECISION,
                    "below52WLowPct"  DOUBLE PRECISION  
                )
            """))
            for _, row in final.iterrows():
                conn.execute(text(f"""
                    INSERT INTO exchange_history."{table}"
                        (time, "advancersPct", "noChangesPct", "declinersPct",
                         "aboveEma20Pct", "aboveEma50Pct", "aboveEma100Pct", "aboveEma200Pct",
                         "aboveRsi50Pct", "aboveRsi70Pct", "belowRsi30Pct", "positiveMacdPct",
                         "above52WHighPct", "below52WLowPct")
                    VALUES
                        (:time, :advancersPct, :noChangesPct, :declinersPct,
                         :aboveEma20Pct, :aboveEma50Pct, :aboveEma100Pct, :aboveEma200Pct,
                         :aboveRsi50Pct, :aboveRsi70Pct, :belowRsi30Pct, :positiveMacdPct,
                         :above52WHighPct, :below52WLowPct)
                    ON CONFLICT (time) DO UPDATE SET
                        "advancersPct"    = EXCLUDED."advancersPct",
                        "noChangesPct"    = EXCLUDED."noChangesPct",
                        "declinersPct"    = EXCLUDED."declinersPct",
                        "aboveEma20Pct"   = EXCLUDED."aboveEma20Pct",
                        "aboveEma50Pct"   = EXCLUDED."aboveEma50Pct",
                        "aboveEma100Pct"  = EXCLUDED."aboveEma100Pct",
                        "aboveEma200Pct"  = EXCLUDED."aboveEma200Pct",
                        "aboveRsi50Pct"   = EXCLUDED."aboveRsi50Pct",
                        "aboveRsi70Pct"   = EXCLUDED."aboveRsi70Pct",
                        "belowRsi30Pct"   = EXCLUDED."belowRsi30Pct",
                        "positiveMacdPct" = EXCLUDED."positiveMacdPct",
                        "above52WHighPct" = EXCLUDED."above52WHighPct",
                        "below52WLowPct"  = EXCLUDED."below52WLowPct"
                """), row.to_dict())
        print(f"Đã upsert {len(final)} dòng vào exchange_history.{table}")

    print("Hoàn tất!")