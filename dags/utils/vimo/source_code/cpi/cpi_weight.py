from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)

SCHEMA = "macro"
TABLE  = "cpi_weight"

# Quyền số rổ CPI (cố định, nguồn GSO)
WEIGHTS = {
    "food_beverage":          33.56,
    "food_staple":             3.67,
    "food":                   21.28,
    "eating_out":              8.61,
    "drink_tobacco":           2.73,
    "clothing":                5.70,
    "housing":                18.82,
    "household":               6.74,
    "healthcare":              5.39,
    "medical_service":         4.11,
    "transport":               9.67,
    "telecom":                 3.14,
    "education":               6.17,
    "education_service":       5.45,
    "culture_entertainment":   4.55,
    "other":                   3.53,
}


def cpi_weight(**context):
    """Upsert quyền số rổ CPI vào DB (chạy 1 lần hoặc khi GSO cập nhật quyền số)."""
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        col_defs = "\n".join([f"    {c} DOUBLE PRECISION," for c in WEIGHTS])
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                id SERIAL PRIMARY KEY,
                effective_date DATE NOT NULL,
{col_defs}
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        for col in WEIGHTS:
            conn.execute(text(f"""
                ALTER TABLE {SCHEMA}.{TABLE}
                ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION
            """))

        cols        = list(WEIGHTS.keys())
        set_clause  = ",\n".join([f"    {c} = EXCLUDED.{c}" for c in cols])
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f":{c}" for c in cols])

        row = {"effective_date": "2020-01-01", **WEIGHTS}
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.{TABLE} (effective_date, {insert_cols})
            VALUES (:effective_date, {insert_vals})
            ON CONFLICT DO NOTHING
        """), row)

    print(f"✅ Upsert quyền số vào {SCHEMA}.{TABLE}")


if __name__ == "__main__":
    cpi_weight()