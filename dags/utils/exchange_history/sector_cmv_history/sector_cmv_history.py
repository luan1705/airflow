import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values


DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
)

OUTPUT_SCHEMA = "exchange_history"


def normalize_free_float(value):
    """
    Chuẩn hóa freeFloatPct về dạng 0-1.

    Ví dụ:
        45   -> 0.45
        0.45 -> 0.45
    """
    if value is None or pd.isna(value):
        return np.nan

    value = float(value)

    if value > 1:
        value /= 100

    if value <= 0:
        return np.nan

    return min(value, 1.0)


def get_meta(exchange: str) -> pd.DataFrame:
    """
    Lấy danh sách cổ phiếu, ngành, số cổ phiếu lưu hành
    và tỷ lệ free-float.
    """
    exchange = exchange.upper()

    with engine.begin() as conn:
        meta = pd.read_sql(
            text("""
                SELECT
                    symbol,
                    sector,
                    "sharesOutstanding",
                    "freeFloatPct"
                FROM info.asset
                WHERE exchange = :exchange
                  AND symbol IS NOT NULL
                  AND sector IS NOT NULL
                  AND sector <> ''
                  AND "sharesOutstanding" IS NOT NULL
                  AND "freeFloatPct" IS NOT NULL
            """),
            conn,
            params={
                "exchange": exchange,
            },
        )

    if meta.empty:
        raise ValueError(
            f"Không có metadata cho sàn {exchange}"
        )

    meta["symbol"] = (
        meta["symbol"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    meta["sector"] = (
        meta["sector"]
        .astype(str)
        .str.strip()
    )

    meta["sharesOutstanding"] = pd.to_numeric(
        meta["sharesOutstanding"],
        errors="coerce",
    )

    meta["free_float"] = (
        meta["freeFloatPct"]
        .apply(normalize_free_float)
    )

    # Số lượng cổ phiếu free-float
    meta["weight"] = (
        meta["sharesOutstanding"]
        * meta["free_float"]
    )

    meta = meta.dropna(
        subset=[
            "symbol",
            "sector",
            "weight",
        ]
    )

    meta = meta[
        meta["weight"] > 0
    ]

    meta = (
        meta
        .drop_duplicates(
            subset=["symbol"],
            keep="last",
        )
        .set_index("symbol")
    )

    if meta.empty:
        raise ValueError(
            f"Không có cổ phiếu hợp lệ cho sàn {exchange}"
        )

    return meta


def load_benchmark_calendar(
    benchmark: str,
) -> pd.DatetimeIndex:
    """
    Benchmark chỉ dùng để lấy lịch giao dịch chuẩn của sàn.
    """
    with engine.begin() as conn:
        df = pd.read_sql(
            text(f"""
                SELECT DISTINCT
                    time::date AS time
                FROM ohlcv."{benchmark}"
                ORDER BY time
            """),
            conn,
        )

    if df.empty:
        raise ValueError(
            f"Không có dữ liệu benchmark {benchmark}"
        )

    calendar = pd.DatetimeIndex(
        pd.to_datetime(
            df["time"],
            errors="coerce",
        ).dropna()
    )

    calendar = (
        calendar
        .sort_values()
        .unique()
    )

    if len(calendar) == 0:
        raise ValueError(
            f"Lịch giao dịch {benchmark} không hợp lệ"
        )

    return pd.DatetimeIndex(calendar)


def load_prices(
    symbols: list[str],
    calendar: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    Đọc toàn bộ lịch sử close của các cổ phiếu,
    pivot thành:

        time | AAA | ACB | BID | ...
    """
    rows = []

    with engine.begin() as conn:
        for index, symbol in enumerate(
            symbols,
            start=1,
        ):
            try:
                symbol_df = pd.read_sql(
                    text(f"""
                        SELECT
                            time::date AS time,
                            close
                        FROM ohlcv."{symbol}_1D"
                        ORDER BY time
                    """),
                    conn,
                )

                if symbol_df.empty:
                    continue

                symbol_df["symbol"] = symbol
                rows.append(symbol_df)

            except Exception as exc:
                print(f"⚠️ {symbol}: {exc}")

            if index % 100 == 0:
                print(
                    f"📥 Đã đọc {index}/{len(symbols)} mã"
                )

    if not rows:
        raise ValueError(
            "Không đọc được lịch sử giá cổ phiếu"
        )

    prices_raw = pd.concat(
        rows,
        ignore_index=True,
    )

    prices_raw["time"] = pd.to_datetime(
        prices_raw["time"],
        errors="coerce",
    )

    prices_raw["close"] = pd.to_numeric(
        prices_raw["close"],
        errors="coerce",
    )

    prices_raw = prices_raw.dropna(
        subset=[
            "time",
            "symbol",
            "close",
        ]
    )

    prices_raw = prices_raw[
        prices_raw["close"] > 0
    ]

    if prices_raw.empty:
        raise ValueError(
            "Không có dữ liệu giá hợp lệ"
        )

    prices = (
        prices_raw
        .pivot_table(
            index="time",
            columns="symbol",
            values="close",
            aggfunc="last",
        )
        .reindex(calendar)
        .ffill()
    )

    prices.index.name = "time"

    return prices


def calculate_sector_cmv_history(
    exchange: str,
    benchmark: str,
) -> pd.DataFrame:
    """
    CMV ngành tại ngày t:

        CMV_sector,t
        =
        Σ(
            close_i,t
            × sharesOutstanding_i
            × freeFloatPct_i
        )

    Kết quả:

        time | Ngân hàng | Bất động sản | Công nghệ | ...
    """
    exchange = exchange.upper()

    meta = get_meta(exchange)

    calendar = load_benchmark_calendar(
        benchmark
    )

    prices = load_prices(
        symbols=meta.index.tolist(),
        calendar=calendar,
    )

    sector_series = []

    for sector, sector_meta in meta.groupby(
        "sector"
    ):
        symbols = [
            symbol
            for symbol in sector_meta.index
            if symbol in prices.columns
        ]

        if not symbols:
            continue

        sector_prices = prices[symbols]

        weights = sector_meta.loc[
            symbols,
            "weight",
        ]

        # close × sharesOutstanding × freeFloatPct
        sector_market_values = (
            sector_prices
            .mul(weights, axis=1)
        )

        # Tổng CMV của toàn ngành theo từng ngày
        sector_cmv = sector_market_values.sum(
            axis=1,
            min_count=1,
        )

        sector_series.append(
            sector_cmv.rename(sector)
        )

    if not sector_series:
        raise ValueError(
            f"Không tính được CMV ngành cho {exchange}"
        )

    result = pd.concat(
        sector_series,
        axis=1,
    )

    result.index.name = "time"

    result = (
        result
        .sort_index()
        .reset_index()
    )

    return result


def ensure_output_table(
    exchange: str,
    sector_columns: list[str],
):
    exchange = exchange.upper()
    table_name = f"sector_cmv_{exchange}"

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE SCHEMA IF NOT EXISTS
            "{OUTPUT_SCHEMA}"
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS
            "{OUTPUT_SCHEMA}"."{table_name}" (
                time DATE PRIMARY KEY
            )
        """))

        for sector in sector_columns:
            conn.execute(text(f"""
                ALTER TABLE
                "{OUTPUT_SCHEMA}"."{table_name}"
                ADD COLUMN IF NOT EXISTS
                "{sector}" DOUBLE PRECISION
            """))


def upsert_sector_cmv_history(
    exchange: str,
    sector_cmv_history: pd.DataFrame,
):
    if sector_cmv_history.empty:
        print(
            f"⚠️ {exchange}: không có dữ liệu CMV ngành"
        )
        return

    exchange = exchange.upper()
    table_name = f"sector_cmv_{exchange}"

    sector_columns = [
        column
        for column in sector_cmv_history.columns
        if column != "time"
    ]

    ensure_output_table(
        exchange=exchange,
        sector_columns=sector_columns,
    )

    data = sector_cmv_history.copy()

    data["time"] = pd.to_datetime(
        data["time"]
    ).dt.date

    data = (
        data.astype(object)
        .where(pd.notna(data), None)
    )

    columns = data.columns.tolist()

    column_list = ", ".join(
        f'"{column}"'
        for column in columns
    )

    update_set = ", ".join(
        f'"{column}" = EXCLUDED."{column}"'
        for column in columns
        if column != "time"
    )

    rows = list(
        data.itertuples(
            index=False,
            name=None,
        )
    )

    if not rows:
        return

    with engine.begin() as conn:
        with conn.connection.cursor() as cursor:
            execute_values(
                cursor,
                f"""
                    INSERT INTO
                    "{OUTPUT_SCHEMA}"."{table_name}"
                    ({column_list})
                    VALUES %s
                    ON CONFLICT (time)
                    DO UPDATE SET
                        {update_set}
                """,
                rows,
                page_size=1000,
            )

    print(
        f'✅ {exchange}: upsert {len(rows)} dòng vào '
        f'"{OUTPUT_SCHEMA}"."{table_name}"'
    )


def sector_cmv_history(
    exchange: str,
    benchmark: str,
):
    sector_cmv_history = calculate_sector_cmv_history(
        exchange=exchange,
        benchmark=benchmark,
    )

    upsert_sector_cmv_history(
        exchange=exchange,
        sector_cmv_history=sector_cmv_history,
    )

    print(
        sector_cmv_history
        .tail(10)
        .to_string(index=False)
    )


def sector_cmv_history_all(**context):
    sector_cmv_history(
        exchange="HOSE",
        benchmark="VNINDEX_1D",
    )

    sector_cmv_history(
        exchange="HNX",
        benchmark="HNXINDEX_1D",
    )

    sector_cmv_history(
        exchange="UPCOM",
        benchmark="UPCOMINDEX_1D",
    )


if __name__ == "__main__":
    sector_cmv_history_all()