from sqlalchemy import create_engine, text
import pandas as pd
import logging

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
    pool_size=10, max_overflow=20, pool_timeout=60
)


def calc_breadth_ma5(index_name: str):
    # Lấy danh sách symbol
    symbols = pd.read_sql(text(f"""
        SELECT symbol FROM info.asset 
        WHERE indices LIKE '%{index_name}%'
    """), engine)['symbol'].tolist()

    if not symbols:
        log.warning(f"Không có symbol cho {index_name}")
        return None

    # Lấy data
    union = " UNION ALL ".join([
        f"SELECT time, '{s}' as symbol, close FROM ohlcv.\"{s}_1D\""
        for s in symbols
    ])
    df = pd.read_sql(f"SELECT * FROM ({union}) t ORDER BY time", engine)
    df['time'] = pd.to_datetime(df['time'], utc=True).dt.normalize().dt.tz_localize(None)
    df = df[df['time'].dt.dayofweek < 5]

    # Tính % tăng/giảm
    close = df.pivot(index='time', columns='symbol', values='close').sort_index()
    chg = close.pct_change(fill_method=None)

    den = chg.notna().sum(axis=1).replace(0, float('nan'))

    result = pd.DataFrame(index=close.index)
    result['advancersPct']      = ((chg > 0).sum(axis=1) / den * 100).round(2)
    result['declinersPct']     = ((chg < 0).sum(axis=1) / den * 100).round(2)
    result['noChangesPct'] = ((chg == 0).sum(axis=1) / den * 100).round(2)
    result['advancersPctMa5']      = result['advancersPct'].rolling(5).mean().round(2)
    result['declinersPctMa5']     = result['declinersPct'].rolling(5).mean().round(2)
    result['noChangesPctMa5'] = result['noChangesPct'].rolling(5).mean().round(2)

    result = result.dropna(subset=['advancersPctMa5']).reset_index()
    return result


def upsert_result(df, index_name):
    table = f'breadth_ma5_{index_name}'
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS exchange_history."{table}" (
                time              DATE PRIMARY KEY,
                "advancersPct"            DOUBLE PRECISION,
                "noChangesPct"       DOUBLE PRECISION,
                "declinersPct"           DOUBLE PRECISION,
                "advancersPctMa5"         DOUBLE PRECISION,
                "noChangesPctMa5"    DOUBLE PRECISION,
                "declinersPctMa5"        DOUBLE PRECISION

            )
        """))
        for _, row in df.iterrows():
            conn.execute(text(f"""
                INSERT INTO exchange_history."{table}"
                    (time, "advancersPct", "declinersPct", "noChangesPct", "advancersPctMa5", "declinersPctMa5", "noChangesPctMa5")
                VALUES
                    (:time, :advancersPct, :declinersPct, :noChangesPct, :advancersPctMa5, :declinersPctMa5, :noChangesPctMa5)
                ON CONFLICT (time) DO UPDATE SET
                    "advancersPct"      = EXCLUDED."advancersPct",
                    "declinersPct"     = EXCLUDED."declinersPct",
                    "noChangesPct" = EXCLUDED."noChangesPct",
                    "advancersPctMa5"      = EXCLUDED."advancersPctMa5",
                    "declinersPctMa5"     = EXCLUDED."declinersPctMa5",
                    "noChangesPctMa5" = EXCLUDED."noChangesPctMa5"
            """), row.to_dict())
    print(f"Đã upsert {len(df)} dòng vào exchange_history.{table}")


def ma5_adv_dec():
    for index_name in ['VN30', 'VNMID', 'VNSMALL','VN100']:
        result = calc_breadth_ma5(index_name)
        if result is not None:
            upsert_result(result, index_name)
    print("Hoàn tất!")