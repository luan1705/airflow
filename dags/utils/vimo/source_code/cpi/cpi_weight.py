from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
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
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        col_defs  = "\n".join([f"    {c} DOUBLE PRECISION," for c in list(WEIGHTS.keys())[:-1]])
        col_defs += f"\n    {list(WEIGHTS.keys())[-1]} DOUBLE PRECISION"
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                id SERIAL PRIMARY KEY,
{col_defs}
            )
        """))
        for col in WEIGHTS:
            conn.execute(text(f"""
                ALTER TABLE {SCHEMA}.{TABLE}
                ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION
            """))

        cols        = list(WEIGHTS.keys())
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f":{c}" for c in cols])

        row = {k: round(v/100, 6) for k, v in WEIGHTS.items()}
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.{TABLE} (id, {insert_cols})
            VALUES (1, {insert_vals})
            ON CONFLICT (id) DO UPDATE SET
                {", ".join([f"{c} = EXCLUDED.{c}" for c in cols])}
        """), row)

    print(f"✅ Upsert quyền số vào {SCHEMA}.{TABLE}")


if __name__ == "__main__":
    cpi_weight()