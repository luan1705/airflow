import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
)

SCHEMA = "macro"
TABLE = "budget_expenditure"

COLS = [
    "recurrent",
    "development_investment",
    "interest_payment",
]


def budget_expenditure_cal(**context):
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

        # Tháng 1 = chính YTD tháng 1
        df.loc[
            month1 & df[ytd_col].notna(),
            col,
        ] = df.loc[
            month1 & df[ytd_col].notna(),
            ytd_col,
        ]

        # Kiểm tra đúng tháng liền trước
        expected_previous_month = (
            df["time"] - pd.DateOffset(months=1)
        ).dt.to_period("M")

        is_previous_month = (
            previous_time.dt.to_period("M")
            == expected_previous_month
        )

        # Tháng 2-12:
        # chỉ tính khi có đúng tháng liền trước
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

    # Tổng chi tháng = tổng 3 cấu phần tháng
    df["total_expenditure"] = (
        pd.to_numeric(df["recurrent"], errors="coerce")
        + pd.to_numeric(df["development_investment"], errors="coerce")
        + pd.to_numeric(df["interest_payment"], errors="coerce")
    )

    # Tổng chi YTD = tổng 3 cấu phần YTD
    df["total_expenditure_ytd"] = (
        pd.to_numeric(df["recurrent_ytd"], errors="coerce")
        + pd.to_numeric(df["development_investment_ytd"], errors="coerce")
        + pd.to_numeric(df["interest_payment_ytd"], errors="coerce")
    )

    df["time"] = df["time"].dt.date

    OUTPUT_COLS = [
        "recurrent",
        "development_investment",
        "interest_payment",
        "total_expenditure",
        "total_expenditure_ytd",
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
        f"✅ Tính xong giá trị tháng "
        f"và tổng chi cho {len(df)} rows"
    )


if __name__ == "__main__":
    budget_expenditure_cal()