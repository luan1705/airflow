import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(DB_URL)

SCHEMA = "macro"
TABLE = "export_country"

COLS = [
    "usa",
    "china",
    "korea",
    "asean",
    "eu",
    "japan",
]


def export_country_cal(**context):
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

    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values("time").reset_index(drop=True)

    # Lấy YTD tháng trước trong cùng năm
    for col in COLS:
        ytd_col = f"{col}_ytd"

        if ytd_col not in df.columns:
            raise ValueError(f"Thiếu cột {ytd_col}")

        df[ytd_col] = pd.to_numeric(
            df[ytd_col],
            errors="coerce",
        )

        previous_ytd = (
            df.groupby(df["time"].dt.year)[ytd_col]
            .shift(1)
        )

        previous_time = (
            df.groupby(df["time"].dt.year)["time"]
            .shift(1)
        )

        month1 = df["time"].dt.month == 1

        # Mặc định NULL
        df[col] = None

        # Tháng 1 = chính YTD tháng 1
        valid_month1 = (
            month1
            & df[ytd_col].notna()
        )

        df.loc[valid_month1, col] = (
            df.loc[valid_month1, ytd_col]
        )

        # Tháng trước đúng nghĩa phải là tháng liền kề
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

        # Chỉ khi có đúng tháng liền trước mới tính
        df.loc[valid, col] = (
            df.loc[valid, ytd_col]
            - previous_ytd.loc[valid]
        )

    df["time"] = df["time"].dt.date

    records = (
        df[["time"] + COLS]
        .astype(object)
        .where(
            pd.notna(df[["time"] + COLS]),
            None,
        )
        .to_dict(orient="records")
    )

    set_clause = ", ".join(
        f'"{col}" = :{col}'
        for col in COLS
    )

    with engine.begin() as conn:
        for row in records:
            conn.execute(
                text(f"""
                    UPDATE "{SCHEMA}"."{TABLE}"
                    SET {set_clause}
                    WHERE time = :time
                """),
                row,
            )

    print(f"✅ Tính xong giá trị tháng cho {len(df)} rows")


if __name__ == "__main__":
    export_country_cal()