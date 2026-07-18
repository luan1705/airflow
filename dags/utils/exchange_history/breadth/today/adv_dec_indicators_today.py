from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    poolclass=NullPool
)

def adv_dec_indicators_today():

    asset = pd.read_sql(text("SELECT symbol, exchange FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')"), engine)
    tables = pd.read_sql(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ohlcv' AND table_name LIKE '%_1D'"), engine)['table_name'].tolist()

    # Chỉ lấy 10 ngày gần nhất
    df_all = pd.concat([
        pd.read_sql(f'SELECT time, close FROM ohlcv."{t}" WHERE time >= CURRENT_DATE - INTERVAL \'600 days\'', engine).assign(symbol=t.replace('_1D', ''))
        for t in tables
    ], ignore_index=True)

    df_all['time'] = pd.to_datetime(df_all['time'], utc=True).dt.normalize().dt.tz_localize(None)
    df_all = df_all[df_all['time'].dt.dayofweek < 5].merge(asset, on='symbol', how='inner').sort_values(['symbol', 'time'])

    today = pd.Timestamp.today().normalize()

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
        adn = adn[adn['time'] == today]

        # Indicators
        den   = close.notna().sum(axis=1).replace(0, float('nan'))
        delta = close.diff()
        gain  = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
        loss  = (-delta.clip(upper=0)).ewm(span=14, adjust=False).mean()
        rsi   = 100 - (100 / (1 + gain / loss))
        macd50  = close.ewm(span=50,  adjust=False).mean() - close.ewm(span=100, adjust=False).mean()
        macd100 = close.ewm(span=100, adjust=False).mean() - close.ewm(span=200, adjust=False).mean()
        high_1w = close.shift(1).rolling(5).max()
        low_1w  = close.shift(1).rolling(5).min()
        high_1m = close.shift(1).rolling(21).max()
        low_1m  = close.shift(1).rolling(21).min()
        high_6m = close.shift(1).rolling(126).max()
        low_6m  = close.shift(1).rolling(126).min()
        high_1y = close.shift(1).rolling(252).max()
        low_1y  = close.shift(1).rolling(252).min()
        high_3y = close.shift(1).rolling(252 * 3).max()
        low_3y  = close.shift(1).rolling(252 * 3).min()

        ind = pd.DataFrame({
            'aboveEma20Pct':   ((close > close.ewm(span=20,  adjust=False).mean()).sum(axis=1) / den * 100).round(2),
            'aboveEma50Pct':   ((close > close.ewm(span=50,  adjust=False).mean()).sum(axis=1) / den * 100).round(2),
            'aboveEma100Pct':  ((close > close.ewm(span=100, adjust=False).mean()).sum(axis=1) / den * 100).round(2),
            'aboveEma200Pct':  ((close > close.ewm(span=200, adjust=False).mean()).sum(axis=1) / den * 100).round(2),
            'above50Rsi14Pct':    ((rsi > 50).sum(axis=1) / den * 100).round(2),
            'above70Rsi14Pct':    ((rsi > 70).sum(axis=1) / den * 100).round(2),
            'below30Rsi14Pct':    ((rsi < 30).sum(axis=1) / den * 100).round(2),
            'positiveMacd50Pct':  ((macd50  > 0).sum(axis=1) / den * 100).round(2),
            'positiveMacd100Pct': ((macd100 > 0).sum(axis=1) / den * 100).round(2),
            'above1WHighPct':     ((close > high_1w).sum(axis=1) / den * 100).round(2),
            'below1WLowPct':      ((close < low_1w).sum(axis=1)  / den * 100).round(2),
            'above1MHighPct':     ((close > high_1m).sum(axis=1) / den * 100).round(2),
            'below1MLowPct':      ((close < low_1m).sum(axis=1)  / den * 100).round(2),
            'above6MHighPct':     ((close > high_6m).sum(axis=1) / den * 100).round(2),
            'below6MLowPct':      ((close < low_6m).sum(axis=1)  / den * 100).round(2),
            'above1YHighPct':     ((close > high_1y).sum(axis=1) / den * 100).round(2),
            'below1YLowPct':      ((close < low_1y).sum(axis=1)  / den * 100).round(2),
            'above3YHighPct':     ((close > high_3y).sum(axis=1) / den * 100).round(2),
            'below3YLowPct':      ((close < low_3y).sum(axis=1)  / den * 100).round(2),
        }, index=close.index).reset_index()
        ind = ind[ind['time'] == today]

        if adn.empty or ind.empty:
            print(f"Không có dữ liệu hôm nay cho {exchange}")
            continue

        final = adn.merge(ind, on='time', how='inner')
        final = final.drop(columns=['advancers', 'decliners', 'noChanges'])

        # Upsert
        table = f'breadth_{exchange}'
        with engine.begin() as conn:
            for _, row in final.iterrows():
                conn.execute(text(f"""
                    INSERT INTO exchange_history."{table}"
                        (time, "advancersPct", "noChangesPct", "declinersPct",
                        "aboveEma20Pct", "aboveEma50Pct", "aboveEma100Pct", "aboveEma200Pct",
                        "above50Rsi14Pct", "above70Rsi14Pct", "below30Rsi14Pct", "positiveMacd50Pct", "positiveMacd100Pct",
                        "above1WHighPct", "below1WLowPct", "above1MHighPct", "below1MLowPct",
                        "above6MHighPct", "below6MLowPct", "above1YHighPct", "below1YLowPct",
                        "above3YHighPct", "below3YLowPct")
                    VALUES
                        (:time, :advancersPct, :noChangesPct, :declinersPct,
                        :aboveEma20Pct, :aboveEma50Pct, :aboveEma100Pct, :aboveEma200Pct,
                        :above50Rsi14Pct, :above70Rsi14Pct, :below30Rsi14Pct, :positiveMacd50Pct, :positiveMacd100Pct,
                        :above1WHighPct, :below1WLowPct, :above1MHighPct, :below1MLowPct,
                        :above6MHighPct, :below6MLowPct, :above1YHighPct, :below1YLowPct,
                        :above3YHighPct, :below3YLowPct)
                    ON CONFLICT (time) DO UPDATE SET
                        "advancersPct"    = EXCLUDED."advancersPct",
                        "noChangesPct"    = EXCLUDED."noChangesPct",
                        "declinersPct"    = EXCLUDED."declinersPct",
                        "aboveEma20Pct"   = EXCLUDED."aboveEma20Pct",
                        "aboveEma50Pct"   = EXCLUDED."aboveEma50Pct",
                        "aboveEma100Pct"  = EXCLUDED."aboveEma100Pct",
                        "aboveEma200Pct"  = EXCLUDED."aboveEma200Pct",
                        "above50Rsi14Pct"    = EXCLUDED."above50Rsi14Pct",
                        "above70Rsi14Pct"    = EXCLUDED."above70Rsi14Pct",
                        "below30Rsi14Pct"    = EXCLUDED."below30Rsi14Pct",
                        "positiveMacd50Pct"  = EXCLUDED."positiveMacd50Pct",
                        "positiveMacd100Pct" = EXCLUDED."positiveMacd100Pct",
                        "above1WHighPct"     = EXCLUDED."above1WHighPct",
                        "below1WLowPct"      = EXCLUDED."below1WLowPct",
                        "above1MHighPct"     = EXCLUDED."above1MHighPct",
                        "below1MLowPct"      = EXCLUDED."below1MLowPct",
                        "above6MHighPct"     = EXCLUDED."above6MHighPct",
                        "below6MLowPct"      = EXCLUDED."below6MLowPct",
                        "above1YHighPct"     = EXCLUDED."above1YHighPct",
                        "below1YLowPct"      = EXCLUDED."below1YLowPct",
                        "above3YHighPct"     = EXCLUDED."above3YHighPct",
                        "below3YLowPct"      = EXCLUDED."below3YLowPct"
                """), row.to_dict())
        print(f"Đã upsert {len(final)} dòng vào exchange_history.{table}")

    print("Hoàn tất!")