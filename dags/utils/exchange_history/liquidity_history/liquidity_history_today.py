import logging
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import sql


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)

DATABASE_URL = (
    "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
)

VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")


def normalize_exchange(exchange: str) -> str:
    exchange = exchange.strip().upper()

    if exchange not in ("HOSE", "HNX", "UPCOM"):
        raise ValueError(exchange)

    return exchange


def get_assets(cursor) -> dict[str, list[str]]:
    """
    Lấy toàn bộ symbol thuộc HOSE, HNX và UPCOM.

    Không lọc active/available vì vẫn cần tính dữ liệu
    của các mã đã hủy niêm yết trong lịch sử.
    """
    cursor.execute("""
        SELECT DISTINCT
            TRIM(symbol) AS symbol,
            UPPER(TRIM(exchange)) AS exchange
        FROM info.asset
        WHERE UPPER(TRIM(exchange)) IN ('HOSE', 'HNX', 'UPCOM')
        ORDER BY exchange, symbol
    """)

    assets_by_exchange = defaultdict(list)

    for symbol, exchange in cursor.fetchall():
        try:
            normalized_exchange = normalize_exchange(exchange)
            normalized_symbol = symbol.strip().upper()

            assets_by_exchange[normalized_exchange].append(
                normalized_symbol
            )

        except (AttributeError, ValueError):
            logger.exception(
                "Bỏ qua asset không hợp lệ: symbol=%s exchange=%s",
                symbol,
                exchange,
            )

    return dict(assets_by_exchange)


def get_existing_ohlcv_tables(cursor) -> set[str]:
    """
    Lấy toàn bộ bảng OHLCV khung 1D một lần.
    """
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'ohlcv'
          AND table_type = 'BASE TABLE'
          AND table_name LIKE %s
    """, ("%_1D",))

    return {row[0] for row in cursor.fetchall()}


def create_target_table(cursor, exchange: str) -> None:
    table_name = f"liquidity_history_{exchange}"

    cursor.execute(
        sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                time   DATE PRIMARY KEY,
                volume DOUBLE PRECISION NOT NULL DEFAULT 0,
                value  DOUBLE PRECISION NOT NULL DEFAULT 0
            )
        """).format(
            sql.Identifier("exchange_history"),
            sql.Identifier(table_name),
        )
    )


def aggregate_exchange_today(
    connection,
    exchange: str,
    symbols: list[str],
    existing_tables: set[str],
    trading_date: str,
) -> None:
    """
    Tổng hợp thanh khoản của một sàn trong đúng một ngày.

    Dữ liệu trong bảng đích sẽ được cập nhật lại bằng ON CONFLICT
    mỗi lần hàm chạy.
    """
    target_table = f"liquidity_history_{exchange}"

    with connection.cursor() as cursor:
        create_target_table(cursor, exchange)

        cursor.execute("""
            CREATE TEMP TABLE liquidity_stage (
                trading_date DATE NOT NULL,
                symbol       TEXT NOT NULL,
                volume       NUMERIC,
                value        NUMERIC
            ) ON COMMIT DROP
        """)

        loaded_symbols = 0
        skipped_symbols = 0
        loaded_rows = 0

        for symbol in symbols:
            source_table = f"{symbol}_1D"

            if source_table not in existing_tables:
                skipped_symbols += 1
                continue

            insert_stage_query = sql.SQL("""
                INSERT INTO liquidity_stage (
                    trading_date,
                    symbol,
                    volume,
                    value
                )
                SELECT
                    (
                        "time" AT TIME ZONE 'Asia/Ho_Chi_Minh'
                    )::date AS trading_date,
                    %s AS symbol,
                    COALESCE(volume, 0) AS volume,
                    COALESCE(value, 0) AS value
                FROM {}.{}
                WHERE "time" >= %s::date
                  AND "time" < %s::date + INTERVAL '1 day'
            """).format(
                sql.Identifier("ohlcv"),
                sql.Identifier(source_table),
            )

            cursor.execute(
                insert_stage_query,
                (
                    symbol,
                    trading_date,
                    trading_date,
                ),
            )

            if cursor.rowcount > 0:
                loaded_symbols += 1
                loaded_rows += cursor.rowcount

        cursor.execute(
            sql.SQL("""
                INSERT INTO {}.{} (
                    time,
                    volume,
                    value
                )
                SELECT
                    %s::date,
                    COALESCE(SUM(volume), 0),
                    COALESCE(SUM(value), 0)
                FROM liquidity_stage

                ON CONFLICT (time)
                DO UPDATE SET
                    volume = EXCLUDED.volume,
                    value = EXCLUDED.value
            """).format(
                sql.Identifier("exchange_history"),
                sql.Identifier(target_table),
            ),
            (trading_date,),
        )

        logger.info(
            "Cập nhật realtime exchange=%s, date=%s, "
            "loaded_symbols=%s, skipped_tables=%s, source_rows=%s",
            exchange,
            trading_date,
            loaded_symbols,
            skipped_symbols,
            loaded_rows,
        )


def liquidity_history_today() -> None:
    """
    Chỉ tổng hợp dữ liệu của ngày hiện tại theo giờ Việt Nam.
    """
    today = datetime.now(VN_TIMEZONE).date().isoformat()

    logger.info(
        "Bắt đầu cập nhật thanh khoản realtime ngày %s",
        today,
    )

    connection = psycopg2.connect(DATABASE_URL)

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE SCHEMA IF NOT EXISTS exchange_history"
            )

            assets_by_exchange = get_assets(cursor)
            existing_tables = get_existing_ohlcv_tables(cursor)

        connection.commit()

        logger.info(
            "Tìm thấy %s exchange và %s bảng OHLCV 1D",
            len(assets_by_exchange),
            len(existing_tables),
        )

        for exchange in ("HOSE", "HNX", "UPCOM"):
            symbols = assets_by_exchange.get(exchange, [])

            if not symbols:
                logger.warning(
                    "Không tìm thấy symbol cho exchange=%s",
                    exchange,
                )
                continue

            try:
                aggregate_exchange_today(
                    connection=connection,
                    exchange=exchange,
                    symbols=symbols,
                    existing_tables=existing_tables,
                    trading_date=today,
                )

                connection.commit()

            except Exception:
                connection.rollback()

                logger.exception(
                    "Lỗi cập nhật realtime exchange=%s date=%s",
                    exchange,
                    today,
                )

        logger.info(
            "Hoàn tất cập nhật thanh khoản realtime ngày %s",
            today,
        )

    except Exception:
        connection.rollback()
        logger.exception(
            "Không thể cập nhật thanh khoản ngày %s",
            today,
        )
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    liquidity_history_today()