import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(DB_URL)

SCHEMA = "macro"
TABLE  = "disbursement"


def disbursement_cal(**context):
    df = pd.read_sql(text(f'SELECT * FROM {SCHEMA}.{TABLE} ORDER BY time'), engine)
    df['time'] = pd.to_datetime(df['time'])
    df['year'] = df['time'].dt.year

    # Fill disbursementPlan từ T1 sang các tháng trong cùng năm
    plan_by_year = df[df['disbursementPlan'].notna()].groupby('year')['disbursementPlan'].first()
    df['disbursementPlan'] = df.apply(
        lambda row: plan_by_year.get(row['year'], row['disbursementPlan']),
        axis=1
    )

    # Tính disbursementPlanRatio = disbursementYtd / disbursementPlan
    df['disbursementPlanRatio'] = (
        df['disbursementYtd'] / df['disbursementPlan']
    ).round(4)

    df['time'] = df['time'].dt.date
    records = df[['time', 'disbursementPlan', 'disbursementPlanRatio']].replace({float('nan'): None}).to_dict(orient='records')

    with engine.begin() as conn:
        for row in records:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.{TABLE}
                SET "disbursementPlan" = :disbursementPlan,
                    "disbursementPlanRatio" = :disbursementPlanRatio
                WHERE time = :time
            """), row)

    print(f"✅ Tính xong disbursementPlan và disbursementPlanRatio cho {len(df)} rows")


if __name__ == "__main__":
    disbursement_cal()