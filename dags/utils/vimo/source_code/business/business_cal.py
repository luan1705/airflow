import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)

SCHEMA = "macro"
TABLE  = "business"


def business_cal(**context):
    df = pd.read_sql(text(f'SELECT * FROM {SCHEMA}.{TABLE} ORDER BY time'), engine)
    df['time'] = pd.to_datetime(df['time'])

    df_lag = df[['time', 'newBusiness', 'exitBusiness']].copy()
    df_lag['time'] = df_lag['time'] + pd.DateOffset(years=1)
    df_lag = df_lag.rename(columns={
        'newBusiness':  'newBusiness_py',
        'exitBusiness': 'exitBusiness_py',
    })

    df = df.merge(df_lag, on='time', how='left')

    df['newBusiness_yoy']  = (df['newBusiness']  / df['newBusiness_py']  - 1).round(4)
    df['exitBusiness_yoy'] = (df['exitBusiness'] / df['exitBusiness_py'] - 1).round(4)

    df['time'] = df['time'].dt.date
    records = df[['time', 'newBusiness_yoy', 'exitBusiness_yoy']].replace({float('nan'): None}).to_dict(orient='records')

    with engine.begin() as conn:
        for row in records:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.{TABLE}
                SET "newBusiness_yoy" = :newBusiness_yoy,
                    "exitBusiness_yoy" = :exitBusiness_yoy
                WHERE time = :time
            """), row)

    print(f"✅ Tính xong YoY cho {len(df)} rows")


if __name__ == "__main__":
    business_cal()