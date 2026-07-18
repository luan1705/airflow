from sqlalchemy import create_engine, text
import pandas as pd
import logging

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    pool_size=10, max_overflow=20, pool_timeout=60
)


def get_value_today(index_name: str):
    symbols = pd.read_sql(text(f"""
        SELECT symbol FROM info.asset
        WHERE indices LIKE '%{index_name}%'
    """), engine)['symbol'].tolist()

    if not symbols:
        return None

    union = " UNION ALL ".join([
        f'SELECT time, "value" FROM ohlcv."{s}_1D" WHERE time >= CURRENT_DATE - INTERVAL \'10 days\''
        for s in symbols
    ])
    df = pd.read_sql(f"SELECT time, SUM(value) as value FROM ({union}) t GROUP BY time", engine)
    df['time'] = pd.to_datetime(df['time'], utc=True).dt.normalize().dt.tz_localize(None)
    df = df[df['time'].dt.dayofweek < 5]
    return df.set_index('time')['value'].rename(index_name)


def calc_breadth_ma5_today(index_name: str):
    symbols = pd.read_sql(text(f"""
        SELECT symbol FROM info.asset
        WHERE indices LIKE '%{index_name}%'
    """), engine)['symbol'].tolist()

    if not symbols:
        log.warning(f"Không có symbol cho {index_name}")
        return None

    union = " UNION ALL ".join([
        f"SELECT time, '{s}' as symbol, close FROM ohlcv.\"{s}_1D\" WHERE time >= CURRENT_DATE - INTERVAL '10 days'"
        for s in symbols
    ])
    df = pd.read_sql(f"SELECT * FROM ({union}) t ORDER BY time", engine)
    df['time'] = pd.to_datetime(df['time'], utc=True).dt.normalize().dt.tz_localize(None)
    df = df[df['time'].dt.dayofweek < 5]

    close = df.pivot(index='time', columns='symbol', values='close').sort_index()
    chg = close.pct_change(fill_method=None)
    den = chg.notna().sum(axis=1).replace(0, float('nan'))

    result = pd.DataFrame(index=close.index)
    result['advancersPct']    = ((chg > 0).sum(axis=1) / den * 100).round(2)
    result['declinersPct']    = ((chg < 0).sum(axis=1) / den * 100).round(2)
    result['noChangesPct']    = ((chg == 0).sum(axis=1) / den * 100).round(2)
    result['advancersPctMa5'] = result['advancersPct'].rolling(5).mean().round(2)
    result['declinersPctMa5'] = result['declinersPct'].rolling(5).mean().round(2)
    result['noChangesPctMa5'] = result['noChangesPct'].rolling(5).mean().round(2)

    today = pd.Timestamp.today().normalize()
    x_days_ago = today - pd.Timedelta(days=3)
    result = result[result.index >= x_days_ago].reset_index()

    if result.empty:
        log.warning(f"Không có dữ liệu hôm nay cho {index_name}")
        return None

    return result


def calc_trading_value_weight_today():
    vnindex = get_value_today('VNINDEX')
    vn30    = get_value_today('VN30')
    vnmid   = get_value_today('VNMID')
    vnsmall = get_value_today('VNSMALL')
    vn100   = get_value_today('VN100')

    if any(x is None for x in [vnindex, vn30, vnmid, vnsmall, vn100]):
        return None

    df = pd.DataFrame({
        'VNINDEX': vnindex,
        'VN30':    vn30,
        'VNMID':   vnmid,
        'VNSMALL': vnsmall,
        'VN100':   vn100,
    }).dropna()

    today = pd.Timestamp.today().normalize()
    x_days_ago = today - pd.Timedelta(days=3)
    df = df[df.index >= x_days_ago]

    if df.empty:
        return None

    return {
        'VN30':    (df['VN30']    / df['VNINDEX'] * 100).round(2).rename('vnindexValuePct'),
        'VNMID':   (df['VNMID']   / df['VNINDEX'] * 100).round(2).rename('vnindexValuePct'),
        'VNSMALL': (df['VNSMALL'] / df['VNINDEX'] * 100).round(2).rename('vnindexValuePct'),
        'VN100':   (df['VN100']   / df['VNINDEX'] * 100).round(2).rename('vnindexValuePct'),
    }


def upsert_result(df, index_name):
    table = f'breadth_ma5_{index_name}'
    df = df.where(pd.notnull(df), None)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            row_dict = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row.items()}
            conn.execute(text(f"""
                INSERT INTO exchange_history."{table}"
                    (time, "advancersPct", "declinersPct", "noChangesPct",
                     "advancersPctMa5", "declinersPctMa5", "noChangesPctMa5",
                     "vnindexValuePct", "tradingValue")
                VALUES
                    (:time, :advancersPct, :declinersPct, :noChangesPct,
                     :advancersPctMa5, :declinersPctMa5, :noChangesPctMa5,
                     :vnindexValuePct, :tradingValue)
                ON CONFLICT (time) DO UPDATE SET
                    "advancersPct"    = EXCLUDED."advancersPct",
                    "declinersPct"    = EXCLUDED."declinersPct",
                    "noChangesPct"    = EXCLUDED."noChangesPct",
                    "advancersPctMa5" = EXCLUDED."advancersPctMa5",
                    "declinersPctMa5" = EXCLUDED."declinersPctMa5",
                    "noChangesPctMa5" = EXCLUDED."noChangesPctMa5",
                    "vnindexValuePct" = EXCLUDED."vnindexValuePct",
                    "tradingValue"    = EXCLUDED."tradingValue"
            """), row_dict)
    print(f"✅ Đã upsert {len(df)} dòng vào exchange_history.{table}")


def ma5_adv_dec_today():
    weights = calc_trading_value_weight_today()
    trading_values = {
        idx: get_value_today(idx)
        for idx in ['VN30', 'VNMID', 'VNSMALL', 'VN100']
    }

    for index_name in ['VN30', 'VNMID', 'VNSMALL', 'VN100']:
        result = calc_breadth_ma5_today(index_name)
        if result is None:
            continue

        result = result.set_index('time')
        result['vnindexValuePct'] = weights[index_name].reindex(result.index) if weights and index_name in weights else None
        result['tradingValue'] = trading_values[index_name].reindex(result.index) if trading_values[index_name] is not None else None
        result = result.reset_index()

        upsert_result(result, index_name)

    print("✅ Hoàn tất!")


if __name__ == "__main__":
    ma5_adv_dec_today()