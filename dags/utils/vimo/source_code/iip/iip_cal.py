import pandas as pd
from sqlalchemy import create_engine, text
from db_config import POSTGRES_URL

engine = create_engine(POSTGRES_URL)

SCHEMA = "macro"
TABLE  = "iip"


def iip_cal(**context):
    df = pd.read_sql(text(f'SELECT * FROM {SCHEMA}.{TABLE} ORDER BY time'), engine)
    df['time'] = pd.to_datetime(df['time'])

    # TB động 3 tháng của total
    df['totalAvg3m'] = df['total'].rolling(3).mean().round(4)

    records = df[['time', 'totalAvg3m']].replace({float('nan'): None}).to_dict(orient='records')

    with engine.begin() as conn:
        for row in records:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.{TABLE}
                SET "totalAvg3m" = :totalAvg3m
                WHERE time = :time
            """), row)

    print(f"✅ Tính xong totalAvg3m")


if __name__ == "__main__":
    iip_cal()