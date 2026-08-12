import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
)

SCHEMA = "macro"
TABLE = "budget_balance"


def budget_balance_cal(**context):
    df = pd.read_sql(
        text(f"""
            SELECT *
            FROM "{SCHEMA}"."{TABLE}"
            ORDER BY time
        """),
        engine,
    )

    if df.empty:
        print(f"⚠️ Không có dữ liệu trong {SCHEMA}.{TABLE}")
        return

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    df = (
        df
        .dropna(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    df["revenue"] = pd.to_numeric(
        df["revenue"],
        errors="coerce",
    )

    df["expenditure"] = pd.to_numeric(
        df["expenditure"],
        errors="coerce",
    )

    # Revenue YTD
    df["revenue_ytd"] = (
        df.groupby(
            df["time"].dt.year
        )["revenue"]
        .cumsum()
    )

    # Expenditure YTD
    df["expenditure_ytd"] = (
        df.groupby(
            df["time"].dt.year
        )["expenditure"]
        .cumsum()
    )

    # Balance YTD
    df["balance_ytd"] = (
        df["revenue_ytd"]
        - df["expenditure_ytd"]
    )

    df["time"] = df["time"].dt.date

    OUTPUT_COLS = [
        "revenue_ytd",
        "expenditure_ytd",
        "balance_ytd",
    ]

    output = df[
        ["time"] + OUTPUT_COLS
    ].astype(object)

    output = output.where(
        pd.notna(output),
        None,
    )

    records = output.to_dict(
        orient="records"
    )

    set_clause = ", ".join(
        f'"{col}" = :{col}'
        for col in OUTPUT_COLS
    )

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                UPDATE "{SCHEMA}"."{TABLE}"
                SET {set_clause}
                WHERE time = :time
            """),
            records,
        )

    print(
        f"✅ Tính xong YTD cho {len(df)} rows"
    )


if __name__ == "__main__":
    budget_balance_cal()