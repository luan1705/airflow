import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)

SCHEMA = "macro"
TABLE  = "trade"


def trade_cal(**context):
    df = pd.read_sql(text(f'SELECT * FROM {SCHEMA}.{TABLE} ORDER BY time'), engine)
    df['time'] = pd.to_datetime(df['time'])
    df['year'] = df['time'].dt.year

    for col, ytd_col in [
        ('tradeBalance',         'tradeBalance_ytd'),
        ('tradeBalanceDomestic', 'tradeBalanceDomestic_ytd'),
        ('tradeBalanceForeign',  'tradeBalanceForeign_ytd'),
    ]:
        df[ytd_col] = df.groupby('year')[col].cumsum()

    df['time'] = df['time'].dt.date
    update_cols = ['tradeBalance_ytd', 'tradeBalanceDomestic_ytd', 'tradeBalanceForeign_ytd']
    records = df[['time'] + update_cols].replace({float('nan'): None}).to_dict(orient='records')

    with engine.begin() as conn:
        for row in records:
            set_clause = ', '.join([f'"{c}" = :{c}' for c in update_cols])
            conn.execute(text(f"""
                UPDATE {SCHEMA}.{TABLE}
                SET {set_clause}
                WHERE time = :time
            """), row)

    print(f"✅ Tính xong YTD cho {len(df)} rows")


if __name__ == "__main__":
    trade_cal()