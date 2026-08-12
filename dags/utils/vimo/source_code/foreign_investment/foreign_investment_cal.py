import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
)

SCHEMA = "macro"
TABLE = "foreign_investment"

COLS = [
    "registered",
    "realized",
]


def foreign_investment_cal(**context):
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

    for col in COLS:
        ytd_col = f"{col}_ytd"

        if ytd_col not in df.columns:
            raise ValueError(
                f"Thiếu cột {ytd_col}"
            )

        df[ytd_col] = pd.to_numeric(
            df[ytd_col],
            errors="coerce",
        )

        previous_ytd = (
            df.groupby(
                df["time"].dt.year
            )[ytd_col]
            .shift(1)
        )

        previous_time = (
            df.groupby(
                df["time"].dt.year
            )["time"]
            .shift(1)
        )

        month1 = (
            df["time"].dt.month == 1
        )

        df[col] = None

        # Tháng 1 = chính YTD
        df.loc[
            month1 & df[ytd_col].notna(),
            col,
        ] = df.loc[
            month1 & df[ytd_col].notna(),
            ytd_col,
        ]

        expected_previous_month = (
            df["time"] - pd.DateOffset(months=1)
        ).dt.to_period("M")

        is_previous_month = (
            previous_time.dt.to_period("M")
            == expected_previous_month
        )

        valid = (
            ~month1
            & is_previous_month
            & df[ytd_col].notna()
            & previous_ytd.notna()
        )

        df.loc[
            valid,
            col,
        ] = (
            df.loc[valid, ytd_col]
            - previous_ytd.loc[valid]
        )

    df["time"] = df["time"].dt.date

    output = df[
        ["time"] + COLS
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
        for col in COLS
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
        f"✅ Tính xong giá trị tháng "
        f"cho {len(df)} rows"
    )


if __name__ == "__main__":
    foreign_investment_cal()