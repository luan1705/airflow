import concurrent.futures
import logging

import pandas as pd
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from datetime import date, timedelta


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

log = logging.getLogger(__name__)


# ============================================================
# DATABASE
# ============================================================

DB_URL = (
    "postgresql+psycopg2://root:Dnl_123456@"
    "tanhungsoft.com:5432/dnl"
)

engine = create_engine(
    DB_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=60,
    pool_pre_ping=True,
    pool_recycle=1800
)


# ============================================================
# CONFIG
# ============================================================

LOOKBACK_DAYS = 5

SYMBOL_WORKERS = 10
EXCHANGE_WORKERS = 3

EXCHANGES = {
    "HOSE": {
        "index": "VNINDEX",
        "table": "impact_HOSE",
    },
    "HNX": {
        "index": "HNXINDEX",
        "table": "impact_HNX",
    },
    "UPCOM": {
        "index": "UPCOMINDEX",
        "table": "impact_UPCOM",
    },
}


# ============================================================
# GET LAST 5 TRADING DAYS
# ============================================================

def get_last_5_trading_days(benchmark):
    """
    Lấy đúng 5 phiên gần nhất có dữ liệu benchmark,
    không vượt quá ngày hôm qua.

    Return:
        list[date]
    """

    yesterday = date.today() - timedelta(days=1)

    with engine.begin() as conn:

        df = pd.read_sql(
            text(f"""
                SELECT DISTINCT
                    time::date AS time
                FROM ohlcv."{benchmark}_1D"
                WHERE time::date <= :yesterday
                ORDER BY time DESC
                LIMIT :limit
            """),
            conn,
            params={
                "yesterday": yesterday,
                "limit": LOOKBACK_DAYS,
            }
        )

    if df.empty:
        return []

    dates = (
        pd.to_datetime(df["time"])
        .dt.date
        .tolist()
    )

    return sorted(dates)


# ============================================================
# LOAD 1 SYMBOL
# ============================================================

def load_symbol_data(
    symbol,
    exchange,
    start_date,
    end_date
):
    """
    Load OHLCV + quarterly marketCap cho 1 symbol.

    Không ghi DB ở đây.

    Chỉ lấy dữ liệu trong khoảng 5 phiên cần backfill.

    Return:
        (DataFrame, status)
    """

    try:

        # ====================================================
        # OHLCV
        # ====================================================

        ohlcv = pd.read_sql(
            text(f"""
                SELECT
                    time::date AS time,
                    "close"
                FROM ohlcv."{symbol}_1D"
                WHERE time::date BETWEEN :start_date
                                      AND :end_date
                ORDER BY time
            """),
            engine,
            params={
                "start_date": start_date,
                "end_date": end_date
            }
        )

        if ohlcv.empty:
            return None, "ohlcv_empty"

        # ====================================================
        # CLEAN CLOSE
        # ====================================================

        ohlcv["close"] = pd.to_numeric(
            ohlcv["close"],
            errors="coerce"
        )

        ohlcv = ohlcv.dropna(
            subset=["close"]
        )

        if ohlcv.empty:
            return None, "ohlcv_empty"

        # ====================================================
        # TIME
        # ====================================================

        ohlcv["time"] = pd.to_datetime(
            ohlcv["time"],
            errors="coerce"
        ).dt.date

        ohlcv = ohlcv.dropna(
            subset=["time"]
        )

        if ohlcv.empty:
            return None, "ohlcv_empty"

        # ====================================================
        # RETURN
        # ====================================================

        # QUAN TRỌNG:
        # Return được tính từ dữ liệu có sẵn của symbol
        # trước khoảng backfill, để phiên đầu tiên trong
        # 5 phiên vẫn có thể tính được return.
        #
        # Vì vậy cần load thêm lịch sử trước start_date.
        #
        # Tuy nhiên phần trên chỉ lấy 5 phiên.
        # Nếu không có phiên trước đó thì phiên đầu tiên
        # sẽ không có return.

        # ----------------------------------------------------
        # LOAD PREVIOUS CLOSE
        # ----------------------------------------------------

        previous_close = pd.read_sql(
            text(f"""
                SELECT
                    time::date AS time,
                    "close"
                FROM ohlcv."{symbol}_1D"
                WHERE time::date < :start_date
                ORDER BY time DESC
                LIMIT 1
            """),
            engine,
            params={
                "start_date": start_date
            }
        )

        if not previous_close.empty:

            previous_close["close"] = pd.to_numeric(
                previous_close["close"],
                errors="coerce"
            )

            previous_close = previous_close.dropna(
                subset=["close"]
            )

            if not previous_close.empty:

                previous_row = pd.DataFrame({
                    "time": [
                        previous_close.iloc[0]["time"]
                    ],
                    "close": [
                        previous_close.iloc[0]["close"]
                    ]
                })

                previous_row["time"] = pd.to_datetime(
                    previous_row["time"]
                ).dt.date

                ohlcv = pd.concat(
                    [
                        previous_row,
                        ohlcv
                    ],
                    ignore_index=True
                )

        # ----------------------------------------------------
        # SORT
        # ----------------------------------------------------

        ohlcv = (
            ohlcv
            .sort_values("time")
            .drop_duplicates(
                subset=["time"],
                keep="last"
            )
            .reset_index(drop=True)
        )

        # ----------------------------------------------------
        # RETURN
        # ----------------------------------------------------

        ohlcv["return"] = (
            ohlcv["close"]
            .pct_change()
        )

        # ----------------------------------------------------
        # CHỈ GIỮ 5 PHIÊN
        # ----------------------------------------------------

        ohlcv = ohlcv[
            ohlcv["time"].between(
                start_date,
                end_date
            )
        ].copy()

        if ohlcv.empty:
            return None, "ohlcv_empty"

        # ====================================================
        # MARKET CAP
        # ====================================================

        quarterly = pd.read_sql(
            text(f"""
                SELECT
                    "yearReport",
                    "lengthReport",
                    "marketCap"
                FROM index."{symbol}"
                WHERE "lengthReport" BETWEEN 1 AND 4
                ORDER BY
                    "yearReport",
                    "lengthReport"
            """),
            engine
        )

        if quarterly.empty:
            return None, "marketcap_empty"

        # ====================================================
        # CLEAN MARKET CAP
        # ====================================================

        quarterly["year"] = pd.to_numeric(
            quarterly["yearReport"],
            errors="coerce"
        )

        quarterly["quarter"] = pd.to_numeric(
            quarterly["lengthReport"],
            errors="coerce"
        )

        quarterly["marketCap"] = pd.to_numeric(
            quarterly["marketCap"],
            errors="coerce"
        )

        quarterly = quarterly.dropna(
            subset=[
                "year",
                "quarter",
                "marketCap"
            ]
        )

        if quarterly.empty:
            return None, "marketcap_empty"

        quarterly["year"] = (
            quarterly["year"]
            .astype("int64")
        )

        quarterly["quarter"] = (
            quarterly["quarter"]
            .astype("int64")
        )

        quarterly = (
            quarterly
            .sort_values(
                ["year", "quarter"]
            )
            .drop_duplicates(
                ["year", "quarter"],
                keep="last"
            )
        )

        # ====================================================
        # YEAR / QUARTER
        # ====================================================

        time_dt = pd.to_datetime(
            ohlcv["time"]
        )

        ohlcv["year"] = (
            time_dt.dt.year
            .astype("int64")
        )

        ohlcv["quarter"] = (
            time_dt.dt.quarter
            .astype("int64")
        )

        # ====================================================
        # QUARTER KEY
        # ====================================================

        ohlcv["quarter_key"] = (
            ohlcv["year"] * 4
            + ohlcv["quarter"]
        ).astype("int64")

        quarterly["quarter_key"] = (
            quarterly["year"] * 4
            + quarterly["quarter"]
        ).astype("int64")

        # ====================================================
        # MERGE ASOF MARKET CAP
        # ====================================================

        quarterly = quarterly.sort_values(
            "quarter_key"
        )

        ohlcv = ohlcv.sort_values(
            "quarter_key"
        )

        ohlcv = pd.merge_asof(
            ohlcv,
            quarterly[
                [
                    "quarter_key",
                    "marketCap"
                ]
            ],
            on="quarter_key",
            direction="backward"
        )

        # ====================================================
        # RESULT
        # ====================================================

        result = ohlcv[
            [
                "time",
                "return",
                "marketCap"
            ]
        ].copy()

        result["symbol"] = symbol

        result = result[
            [
                "time",
                "symbol",
                "return",
                "marketCap"
            ]
        ]

        return result, None

    except Exception as e:

        log.error(
            "❌ %s %s: %s",
            exchange,
            symbol,
            e
        )

        return None, "error"


# ============================================================
# UPSERT
# ============================================================

def upsert_impact(
    out,
    output_table,
    exchange
):
    """
    Upsert toàn bộ exchange một lần.

    Schema chỉ có:
        time
        symbol
        impact
    """

    if out.empty:
        return

    with engine.begin() as conn:

        # ====================================================
        # CREATE TABLE
        # ====================================================

        conn.execute(
            text(f"""
                CREATE TABLE IF NOT EXISTS
                exchange_history."{output_table}" (
                    time DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    impact DOUBLE PRECISION,
                    PRIMARY KEY (time, symbol)
                )
            """)
        )

        # ====================================================
        # PREPARE ROWS
        # ====================================================

        rows = list(
            out[
                [
                    "time",
                    "symbol",
                    "impact"
                ]
            ].itertuples(
                index=False,
                name=None
            )
        )

        if not rows:
            return

        # ====================================================
        # UPSERT
        # ====================================================

        cur = conn.connection.cursor()

        try:

            execute_values(
                cur,
                f"""
                    INSERT INTO
                    exchange_history."{output_table}"
                    (
                        "time",
                        "symbol",
                        "impact"
                    )
                    VALUES %s

                    ON CONFLICT (
                        "time",
                        "symbol"
                    )

                    DO UPDATE SET
                        "impact" = EXCLUDED."impact"
                """,
                rows,
                page_size=100000
            )

        finally:

            cur.close()

    log.info(
        "💾 %s: đã lưu %d rows → "
        "exchange_history.%s",
        exchange,
        len(rows),
        output_table
    )


# ============================================================
# PROCESS 1 EXCHANGE
# ============================================================

def process_exchange(
    exchange,
    config,
    asset_df,
    ohlcv_symbols,
    index_symbols,
    dates
):

    index_symbol = config["index"]
    output_table = config["table"]

    start_date = min(dates)
    end_date = max(dates)

    # ========================================================
    # HEADER
    # ========================================================

    log.info(
        "=================================================="
    )

    log.info(
        "📊 BẮT ĐẦU %s → %s",
        exchange,
        index_symbol
    )

    log.info(
        "📅 %s: %s → %s (%d phiên)",
        exchange,
        start_date,
        end_date,
        len(dates)
    )

    # ========================================================
    # SYMBOLS
    # ========================================================

    symbols = asset_df.loc[
        asset_df["exchange"] == exchange,
        "symbol"
    ].tolist()

    log.info(
        "📌 %s có %d mã",
        exchange,
        len(symbols)
    )

    if not symbols:

        raise RuntimeError(
            f"{exchange}: không có symbol"
        )

    # ========================================================
    # INDEX POINT
    # ========================================================

    index_df = pd.read_sql(
        text(f"""
            SELECT
                time::date AS time,
                "close" AS point
            FROM ohlcv."{index_symbol}_1D"
            WHERE time::date BETWEEN :start_date
                                  AND :end_date
            ORDER BY time
        """),
        engine,
        params={
            "start_date": start_date,
            "end_date": end_date
        }
    )

    if index_df.empty:

        raise RuntimeError(
            f"{exchange}: không có dữ liệu {index_symbol}"
        )

    # ========================================================
    # FIX TIME
    # ========================================================

    index_df["time"] = pd.to_datetime(
        index_df["time"],
        errors="coerce"
    ).dt.date

    index_df = index_df.dropna(
        subset=["time"]
    )

    # ========================================================
    # CLEAN POINT
    # ========================================================

    index_df["point"] = pd.to_numeric(
        index_df["point"],
        errors="coerce"
    )

    index_df = index_df.dropna(
        subset=["point"]
    )

    # Chỉ giữ đúng các phiên benchmark đã xác định
    index_df = index_df[
        index_df["time"].isin(dates)
    ].copy()

    if index_df.empty:

        raise RuntimeError(
            f"{exchange}: index không có point hợp lệ"
        )

    # ========================================================
    # VALID SYMBOLS
    # ========================================================

    valid_symbols = [
        symbol
        for symbol in symbols
        if symbol in ohlcv_symbols
        and symbol in index_symbols
    ]

    skipped_ohlcv = sum(
        symbol not in ohlcv_symbols
        for symbol in symbols
    )

    skipped_index = sum(
        symbol not in index_symbols
        for symbol in symbols
    )

    log.info(
        "⚡ %s: chạy song song %d/%d mã với %d workers",
        exchange,
        len(valid_symbols),
        len(symbols),
        SYMBOL_WORKERS
    )

    # ========================================================
    # LOAD SYMBOLS
    # ========================================================

    exchange_data = []

    skipped_marketcap = 0
    errors = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=SYMBOL_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                load_symbol_data,
                symbol,
                exchange,
                start_date,
                end_date
            ): symbol
            for symbol in valid_symbols
        }

        for future in concurrent.futures.as_completed(
            futures
        ):

            symbol = futures[future]

            try:

                result, status = future.result()

                if result is not None:

                    exchange_data.append(
                        result
                    )

                elif status == "marketcap_empty":

                    skipped_marketcap += 1

                elif status == "error":

                    errors += 1

            except Exception:

                errors += 1

                log.exception(
                    "❌ Worker %s lỗi",
                    symbol
                )

    # ========================================================
    # SUMMARY
    # ========================================================

    log.info(
        "📊 %s: OHLCV thiếu %d | "
        "index thiếu %d | "
        "marketCap thiếu %d | "
        "lỗi %d",
        exchange,
        skipped_ohlcv,
        skipped_index,
        skipped_marketcap,
        errors
    )

    if not exchange_data:

        raise RuntimeError(
            f"{exchange}: không có historical data"
        )

    # ========================================================
    # CONCAT
    # ========================================================

    data = pd.concat(
        exchange_data,
        ignore_index=True
    )

    del exchange_data

    # ========================================================
    # FIX TIME
    # ========================================================

    data["time"] = pd.to_datetime(
        data["time"],
        errors="coerce"
    ).dt.date

    index_df["time"] = pd.to_datetime(
        index_df["time"],
        errors="coerce"
    ).dt.date

    data = data.dropna(
        subset=["time"]
    )

    index_df = index_df.dropna(
        subset=["time"]
    )

    # ========================================================
    # MERGE INDEX
    # ========================================================

    data = data.merge(
        index_df[
            [
                "time",
                "point"
            ]
        ],
        on="time",
        how="inner"
    )

    if data.empty:

        raise RuntimeError(
            f"{exchange}: "
            f"không có data sau khi merge index"
        )

    # ========================================================
    # TOTAL MARKET CAP
    # ========================================================

    data["totalMarketCap"] = (
        data
        .groupby("time")["marketCap"]
        .transform("sum")
    )

    # ========================================================
    # WEIGHT
    # ========================================================

    data["weight"] = (
        data["marketCap"]
        / data["totalMarketCap"]
    )

    # ========================================================
    # IMPACT
    # ========================================================

    data["impact"] = (
        data["weight"]
        * data["return"]
        * data["point"]
    )

    data["impact"] = (
        pd.to_numeric(
            data["impact"],
            errors="coerce"
        )
        .round(2)
    )

    # ========================================================
    # OUTPUT
    # ========================================================

    out = data[
        [
            "time",
            "symbol",
            "impact"
        ]
    ].copy()

    out = out[
        out["time"].isin(dates)
    ]

    out = out.dropna(
        subset=[
            "time",
            "symbol",
            "impact"
        ]
    )

    if out.empty:

        raise RuntimeError(
            f"{exchange}: "
            f"không có impact hợp lệ"
        )

    # ========================================================
    # UPSERT
    # ========================================================

    log.info(
        "💾 %s: chuẩn bị lưu %d rows",
        exchange,
        len(out)
    )

    upsert_impact(
        out,
        output_table,
        exchange
    )

    log.info(
        "✅ %s: hoàn tất",
        exchange
    )

    # ========================================================
    # CLEAN
    # ========================================================

    del data
    del out


# ============================================================
# MAIN
# ============================================================

def impact_history():

    # ========================================================
    # GET 5 TRADING DAYS
    # ========================================================

    # Dùng benchmark HOSE để xác định 5 phiên gần nhất.
    #
    # Vì VNINDEX là benchmark chính của HOSE và lịch giao dịch
    # của 3 sàn về cơ bản cùng ngày.
    #
    # Sau đó từng exchange vẫn lấy benchmark riêng của nó.

    dates = get_last_5_trading_days(
        EXCHANGES["HOSE"]["index"]
    )

    if not dates:

        raise RuntimeError(
            "Không tìm được 5 phiên giao dịch gần nhất"
        )

    log.info(
        "🚀 Bắt đầu backfill %d phiên gần nhất đến hôm qua",
        LOOKBACK_DAYS
    )

    log.info(
        "📅 Các phiên: %s",
        ", ".join(
            str(d)
            for d in dates
        )
    )

    # ========================================================
    # INDEX TABLES
    # ========================================================

    index_tables = pd.read_sql(
        text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'index'
        """),
        engine
    )["table_name"].tolist()

    index_symbols = set(
        index_tables
    )

    log.info(
        "📚 Có %d bảng index",
        len(index_symbols)
    )

    # ========================================================
    # ASSET
    # ========================================================

    asset_df = pd.read_sql(
        text("""
            SELECT
                symbol,
                exchange
            FROM info.asset
            WHERE exchange IN (
                'HOSE',
                'HNX',
                'UPCOM'
            )
            AND type = 'Stock'
            ORDER BY
                exchange,
                symbol
        """),
        engine
    )

    if asset_df.empty:

        raise RuntimeError(
            "Không có stock trong info.asset"
        )

    # ========================================================
    # OHLCV TABLES
    # ========================================================

    ohlcv_tables = pd.read_sql(
        text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'ohlcv'
        """),
        engine
    )["table_name"].tolist()

    ohlcv_symbols = {
        table_name[:-3]
        for table_name in ohlcv_tables
        if table_name.endswith("_1D")
    }

    log.info(
        "📚 Có %d bảng OHLCV 1D",
        len(ohlcv_symbols)
    )

    # ========================================================
    # PROCESS 3 EXCHANGES IN PARALLEL
    # ========================================================

    log.info(
        "🚀 Chạy song song %d exchange",
        len(EXCHANGES)
    )

    failed_exchanges = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=EXCHANGE_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                process_exchange,
                exchange,
                config,
                asset_df,
                ohlcv_symbols,
                index_symbols,
                dates
            ): exchange
            for exchange, config in EXCHANGES.items()
        }

        for future in concurrent.futures.as_completed(
            futures
        ):

            exchange = futures[future]

            try:

                future.result()

            except Exception:

                failed_exchanges.append(
                    exchange
                )

                log.exception(
                    "❌ %s FAILED",
                    exchange
                )

    # ========================================================
    # FINAL STATUS
    # ========================================================

    if failed_exchanges:

        failed_exchanges = sorted(
            failed_exchanges
        )

        raise RuntimeError(
            "Backfill thất bại ở exchange: "
            + ", ".join(
                failed_exchanges
            )
        )

    log.info(
        "🎯🎯🎯 Hoàn tất backfill %d phiên gần nhất đến hôm qua",
        LOOKBACK_DAYS
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    impact_history()