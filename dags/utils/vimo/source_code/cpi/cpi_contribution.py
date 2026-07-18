import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(DB_URL)

SCHEMA = "macro"
TABLE  = "cpi_contribution"


def cpi_contribution(**context):
    """Tính contribution = (ytd * weight).
    Weight lấy từ bảng cpi_weight, YTD lấy từ bảng cpi_ytd.
    """
    # Lấy weight mới nhất
    df_weight = pd.read_sql(text(f"SELECT * FROM {SCHEMA}.cpi_weight LIMIT 1"), engine)
    if df_weight.empty:
        raise ValueError("Không có dữ liệu trong bảng cpi_weight")

    # Lấy các cột weight (bỏ id, effective_date)
    weight_cols = [c for c in df_weight.columns if c not in ("id", "effective_date")]
    weights = df_weight[weight_cols].iloc[0].to_dict()

    # Lấy YTD
    df_ytd = pd.read_sql(text(f"SELECT * FROM {SCHEMA}.cpi_ytd ORDER BY time"), engine)

    # Tính contribution
    for col, weight in weights.items():
        if col in df_ytd.columns:
            df_ytd[col] = (df_ytd[col] * weight).round(6)

    cols = list(weights.keys())
    df_result = df_ytd[["time"] + [c for c in cols if c in df_ytd.columns]]

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        result_cols = [c for c in cols if c in df_ytd.columns]
        col_defs  = "\n".join([f"    {c} DOUBLE PRECISION," for c in result_cols[:-1]])
        col_defs += f"\n    {result_cols[-1]} DOUBLE PRECISION"
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                time DATE PRIMARY KEY,
{col_defs}
            )
        """))
        for col in result_cols:
            conn.execute(text(f"""
                ALTER TABLE {SCHEMA}.{TABLE}
                ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION
            """))

        set_clause  = ",\n".join([f"    {c} = EXCLUDED.{c}" for c in result_cols])
        insert_cols = ", ".join(result_cols)
        insert_vals = ", ".join([f":{c}" for c in result_cols])
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.{TABLE} (time, {insert_cols})
            VALUES (:time, {insert_vals})
            ON CONFLICT (time) DO UPDATE SET
{set_clause}
        """), df_result.replace({float('nan'): None}).to_dict(orient="records"))

    print(f"✅ Tính xong {len(df_result)} rows vào {SCHEMA}.{TABLE}")


if __name__ == "__main__":
    cpi_contribution()