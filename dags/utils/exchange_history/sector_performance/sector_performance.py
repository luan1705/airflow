import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values


DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
)

OUTPUT_SCHEMA = "sector_performance"

# Đọc dư lịch sử để đủ tính 10 năm
LOOKBACK_YEARS = 11


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
    with engine.begin() as conn:
        meta = pd.read_sql(
            text("""
                SELECT
                    a.symbol,
                    a."sharesOutstanding",
                    a."freeFloatPct",
                    a.sector
                FROM info.asset a
                WHERE a.exchange = :exchange
                  AND a.sector IS NOT NULL
                  AND a.sector <> ''
                  AND a."sharesOutstanding" IS NOT NULL
                  AND a."freeFloatPct" IS NOT NULL
            """),
            conn,
            params={"exchange": exchange},
        )

    if meta.empty:
        raise ValueError(
            f"Không có metadata cổ phiếu cho sàn {exchange}"
        )

    meta["symbol"] = (
        meta["symbol"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    meta["sharesOutstanding"] = pd.to_numeric(
        meta["sharesOutstanding"],
        errors="coerce",
    )

    meta["free_float"] = (
        meta["freeFloatPct"]
        .apply(normalize_free_float)
    )

    # Số cổ phiếu free-float
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

    meta = meta[meta["weight"] > 0]

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


def load_benchmark(benchmark: str) -> pd.Series:
    """
    Benchmark chỉ dùng để lấy lịch giao dịch chuẩn của sàn.
    """
    with engine.begin() as conn:
        benchmark_df = pd.read_sql(
            text(f"""
                SELECT
                    time::date AS date,
                    close
                FROM ohlcv."{benchmark}"
                ORDER BY time
            """),
            conn,
        )

    if benchmark_df.empty:
        raise ValueError(
            f"Không có dữ liệu benchmark {benchmark}"
        )

    benchmark_df["date"] = pd.to_datetime(
        benchmark_df["date"],
        errors="coerce",
    )

    benchmark_df["close"] = pd.to_numeric(
        benchmark_df["close"],
        errors="coerce",
    )

    benchmark_df = (
        benchmark_df
        .dropna(subset=["date", "close"])
        .drop_duplicates(
            subset=["date"],
            keep="last",
        )
        .sort_values("date")
    )

    if benchmark_df.empty:
        raise ValueError(
            f"Dữ liệu benchmark {benchmark} không hợp lệ"
        )

    return benchmark_df.set_index("date")["close"]


def load_prices(
    symbols: list[str],
    calendar: pd.DatetimeIndex,
    start_date: pd.Timestamp,
) -> pd.DataFrame:
    rows = []

    with engine.begin() as conn:
        for index, symbol in enumerate(symbols, start=1):
            try:
                symbol_df = pd.read_sql(
                    text(f"""
                        SELECT
                            time::date AS date,
                            close
                        FROM ohlcv."{symbol}_1D"
                        WHERE time >= :start_date
                        ORDER BY time
                    """),
                    conn,
                    params={
                        "start_date": start_date.date(),
                    },
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
            "Không đọc được dữ liệu giá cổ phiếu"
        )

    prices_raw = pd.concat(
        rows,
        ignore_index=True,
    )

    prices_raw["date"] = pd.to_datetime(
        prices_raw["date"],
        errors="coerce",
    )

    prices_raw["close"] = pd.to_numeric(
        prices_raw["close"],
        errors="coerce",
    )

    prices_raw = prices_raw.dropna(
        subset=["date", "symbol", "close"]
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
            index="date",
            columns="symbol",
            values="close",
            aggfunc="last",
        )
        .reindex(calendar)
        .ffill()
    )

    return prices


def get_base_trading_date(
    calendar: pd.DatetimeIndex,
    period_start: pd.Timestamp,
):
    """
    Lấy phiên giao dịch liền trước ngày đầu kỳ.
    """
    previous_dates = calendar[
        calendar < period_start
    ]

    if len(previous_dates) == 0:
        return None

    return previous_dates[-1]


def build_interval_dates(
    latest_date: pd.Timestamp,
    calendar: pd.DatetimeIndex,
) -> dict:
    period_starts = {
        "today": latest_date,

        "week_1": (
            latest_date
            - pd.DateOffset(weeks=1)
        ),

        "month_1": (
            latest_date
            - pd.DateOffset(months=1)
        ),

        "month_6": (
            latest_date
            - pd.DateOffset(months=6)
        ),

        "ytd": pd.Timestamp(
            year=latest_date.year,
            month=1,
            day=1,
        ),

        "year_1": (
            latest_date
            - pd.DateOffset(years=1)
        ),

        "year_5": (
            latest_date
            - pd.DateOffset(years=5)
        ),

        "year_10": (
            latest_date
            - pd.DateOffset(years=10)
        ),
    }

    return {
        interval: get_base_trading_date(
            calendar=calendar,
            period_start=period_start,
        )
        for interval, period_start
        in period_starts.items()
    }


def calculate_one_sector_interval(
    sector_meta: pd.DataFrame,
    prices: pd.DataFrame,
    base_date,
    latest_date,
):
    """
    Hiệu suất ngành:

        CMV hiện tại / CMV tại phiên trước ngày đầu kỳ - 1

    Trong đó:

        CMV = tổng(close × sharesOutstanding × freeFloatPct)
    """
    if base_date is None:
        return None

    if (
        base_date not in prices.index
        or latest_date not in prices.index
    ):
        return None

    eligible = [
        symbol
        for symbol in sector_meta.index
        if symbol in prices.columns
    ]

    if not eligible:
        return None

    base_prices = prices.loc[
        base_date,
        eligible,
    ]

    latest_prices = prices.loc[
        latest_date,
        eligible,
    ]

    weights = sector_meta.loc[
        eligible,
        "weight",
    ]

    valid = (
        base_prices.notna()
        & latest_prices.notna()
        & weights.notna()
        & (base_prices > 0)
        & (latest_prices > 0)
        & (weights > 0)
    )

    valid_symbols = valid[
        valid
    ].index.tolist()

    if not valid_symbols:
        return None

    base_market_value = (
        base_prices.loc[valid_symbols]
        * weights.loc[valid_symbols]
    ).sum()

    current_market_value = (
        latest_prices.loc[valid_symbols]
        * weights.loc[valid_symbols]
    ).sum()

    if (
        pd.isna(base_market_value)
        or base_market_value <= 0
        or pd.isna(current_market_value)
        or current_market_value <= 0
    ):
        return None

    return float(
        current_market_value
        / base_market_value
        - 1
    )


def calculate_sector_performance(
    exchange: str,
    benchmark: str,
) -> pd.DataFrame:
    exchange = exchange.upper()

    meta = get_meta(exchange)
    benchmark_series = load_benchmark(benchmark)

    latest_date = benchmark_series.index.max()

    start_date = (
        latest_date
        - pd.DateOffset(years=LOOKBACK_YEARS)
        - pd.DateOffset(days=30)
    )

    calendar = benchmark_series.loc[
        benchmark_series.index >= start_date
    ].index

    if len(calendar) == 0:
        raise ValueError(
            f"Không có lịch giao dịch cho sàn {exchange}"
        )

    latest_date = calendar[-1]

    prices = load_prices(
        symbols=meta.index.tolist(),
        calendar=calendar,
        start_date=start_date,
    )

    base_dates = build_interval_dates(
        latest_date=latest_date,
        calendar=calendar,
    )

    results = []

    for sector, sector_meta in meta.groupby("sector"):
        row = {
            "sector": sector,
        }

        for interval, base_date in base_dates.items():
            row[interval] = (
                calculate_one_sector_interval(
                    sector_meta=sector_meta,
                    prices=prices,
                    base_date=base_date,
                    latest_date=latest_date,
                )
            )

        results.append(row)

    return pd.DataFrame(results)


def ensure_output_table(exchange: str):
    exchange = exchange.upper()
    table_name = f"sector_performance_{exchange}"

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE SCHEMA IF NOT EXISTS
            "{OUTPUT_SCHEMA}"
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS
            "{OUTPUT_SCHEMA}"."{table_name}" (
                sector       TEXT PRIMARY KEY,
                today        DOUBLE PRECISION,
                week_1       DOUBLE PRECISION,
                month_1      DOUBLE PRECISION,
                month_6      DOUBLE PRECISION,
                ytd          DOUBLE PRECISION,
                year_1       DOUBLE PRECISION,
                year_5       DOUBLE PRECISION,
                year_10      DOUBLE PRECISION
            )
        """))


def upsert_sector_performance(
    exchange: str,
    performance: pd.DataFrame,
):
    if performance.empty:
        print(
            f"⚠️ {exchange}: không có dữ liệu hiệu suất"
        )
        return

    exchange = exchange.upper()
    table_name = f"sector_performance_{exchange}"

    ensure_output_table(exchange)

    columns = [
        "sector",
        "today",
        "week_1",
        "month_1",
        "month_6",
        "ytd",
        "year_1",
        "year_5",
        "year_10",
    ]

    data = (
        performance[columns]
        .astype(object)
        .where(
            pd.notna(performance[columns]),
            None,
        )
    )

    rows = list(
        data.itertuples(
            index=False,
            name=None,
        )
    )

    with engine.begin() as conn:
        with conn.connection.cursor() as cursor:
            execute_values(
                cursor,
                f"""
                    INSERT INTO
                    "{OUTPUT_SCHEMA}"."{table_name}" (
                        sector,
                        today,
                        week_1,
                        month_1,
                        month_6,
                        ytd,
                        year_1,
                        year_5,
                        year_10
                    )
                    VALUES %s
                    ON CONFLICT (sector)
                    DO UPDATE SET
                        today = EXCLUDED.today,
                        week_1 = EXCLUDED.week_1,
                        month_1 = EXCLUDED.month_1,
                        month_6 = EXCLUDED.month_6,
                        ytd = EXCLUDED.ytd,
                        year_1 = EXCLUDED.year_1,
                        year_5 = EXCLUDED.year_5,
                        year_10 = EXCLUDED.year_10
                """,
                rows,
                page_size=1000,
            )

    print(
        f'✅ {exchange}: upsert {len(rows)} ngành vào '
        f'"{OUTPUT_SCHEMA}"."{table_name}"'
    )


def sector_performance(
    exchange: str,
    benchmark: str,
):
    performance = calculate_sector_performance(
        exchange=exchange,
        benchmark=benchmark,
    )

    upsert_sector_performance(
        exchange=exchange,
        performance=performance,
    )

    print(
        performance
        .sort_values(
            by="today",
            ascending=False,
            na_position="last",
        )
        .to_string(index=False)
    )


def sector_performance_all(**context):
    sector_performance(
        exchange="HOSE",
        benchmark="VNINDEX_1D",
    )

    sector_performance(
        exchange="HNX",
        benchmark="HNXINDEX_1D",
    )

    sector_performance(
        exchange="UPCOM",
        benchmark="UPCOMINDEX_1D",
    )


if __name__ == "__main__":
    sector_performance_all()