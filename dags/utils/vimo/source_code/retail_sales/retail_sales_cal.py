import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)

SCHEMA = "macro"
TABLE  = "retail_sales"

COLS = ["total", "retail", "accommodation", "tourism", "other"]


def retail_sales_cal(**context):
    # Đọc dữ liệu
    df = pd.read_sql(text(f"SELECT * FROM {SCHEMA}.{TABLE} ORDER BY time"), engine)
    df["time"] = pd.to_datetime(df["time"])

    # YoY
    df_lag = df[["time"] + COLS].copy()
    df_lag["time"] = df_lag["time"] + pd.DateOffset(years=1)
    df_lag = df_lag.rename(columns={c: f"{c}_py" for c in COLS})

    df = df.merge(df_lag, on="time", how="left")

    for c in COLS:
        df[f"{c}_yoy"] = df[c] / df[f"{c}_py"] - 1

    # YTD
    df["year"] = df["time"].dt.year
    for c in COLS:
        df[f"{c}_ytd"] = df.groupby("year")[c].cumsum()

    # YTD YoY
    df_ytd_lag = df[["time"] + [f"{c}_ytd" for c in COLS]].copy()
    df_ytd_lag["time"] = df_ytd_lag["time"] + pd.DateOffset(years=1)
    df_ytd_lag = df_ytd_lag.rename(columns={f"{c}_ytd": f"{c}_ytd_py" for c in COLS})

    df = df.merge(df_ytd_lag, on="time", how="left")

    for c in COLS:
        df[f"{c}_ytd_yoy"] = df[f"{c}_ytd"] / df[f"{c}_ytd_py"] - 1

    # Upsert lại
    update_cols = (
        [f"{c}_yoy" for c in COLS] +
        [f"{c}_ytd" for c in COLS] +
        [f"{c}_ytd_yoy" for c in COLS]
    )
    df["time"] = df["time"].dt.date

    # Thay NaN thành None để không ghi vào DB
    records = df[["time"] + update_cols].replace({float('nan'): None}).to_dict(orient="records")


    with engine.begin() as conn:
        for row in records:
            set_clause = ", ".join([f"{col} = :{col}" for col in update_cols])
            conn.execute(text(f"""
                UPDATE {SCHEMA}.{TABLE}
                SET {set_clause}
                WHERE time = :time
            """), row)

    print(f"✅ Tính xong {len(df)} rows")


if __name__ == "__main__":
    retail_sales_cal()