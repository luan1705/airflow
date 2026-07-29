import logging
import re
from collections import defaultdict

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


def normalize_exchange(exchange: str) -> str:
    exchange = exchange.strip().upper()

    if exchange not in ("HOSE", "HNX", "UPCOM"):
        raise ValueError(exchange)

    return exchange


def get_assets(cursor) -> dict[str, list[str]]:
    """
    Lấy toàn bộ symbol có exchange.

    Không lọc active/available vì đang tính lịch sử.
    Nếu lọc, các mã đã hủy niêm yết sẽ bị mất khỏi lịch sử thanh khoản.
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
            assets_by_exchange[normalized_exchange].append(symbol)
        except ValueError:
            logger.exception(
                "Bỏ qua asset không hợp lệ: symbol=%s exchange=%s",
                symbol,
                exchange,
            )

    return dict(assets_by_exchange)


def get_existing_ohlcv_tables(cursor) -> set[str]:
    """
    Lấy toàn bộ tên bảng OHLCV một lần để tránh query
    information_schema cho từng symbol.
    """
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'ohlcv'
          AND table_type = 'BASE TABLE'
          AND table_name LIKE %s
    """, ("%_1D",))

    return {row[0] for row in cursor.fetchall()}


def create_target_table(cursor, exchange: str):
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


def aggregate_exchange(
    connection,
    exchange: str,
    symbols: list[str],
    existing_tables: set[str],
    from_date=None,
    to_date=None,
) -> None:
    """
    Tổng hợp volume và value của toàn bộ symbol thuộc một exchange.

    Khoảng ngày:
        from_date <= trading_date <= to_date

    from_date, to_date:
        YYYY-MM-DD
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

                logger.warning(
                    "Không tồn tại bảng ohlcv.%s",
                    source_table,
                )
                continue

            conditions = []
            parameters = [symbol]

            # So sánh trực tiếp trên time để PostgreSQL dùng index PK.
            if from_date is not None:
                conditions.append(
                    sql.SQL(
                        '"time" >= %s::date'
                    )
                )
                parameters.append(from_date)

            if to_date is not None:
                conditions.append(
                    sql.SQL(
                        '"time" < %s::date + INTERVAL \'1 day\''
                    )
                )
                parameters.append(to_date)

            where_clause = sql.SQL("")

            if conditions:
                where_clause = sql.SQL("WHERE {}").format(
                    sql.SQL(" AND ").join(conditions)
                )

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
                    value
                FROM {}.{}
                {}
            """).format(
                sql.Identifier("ohlcv"),
                sql.Identifier(source_table),
                where_clause,
            )

            cursor.execute(
                insert_stage_query,
                parameters,
            )

            loaded_symbols += 1
            loaded_rows += cursor.rowcount

        upsert_query = sql.SQL("""
            INSERT INTO {}.{} (
                time,
                volume,
                value
            )
            SELECT
                trading_date,
                COALESCE(SUM(volume),0),
                COALESCE(SUM(value),0)
            FROM liquidity_stage
            GROUP BY trading_date

            ON CONFLICT (time)
            DO UPDATE SET
                volume = EXCLUDED.volume,
                value = EXCLUDED.value
        """).format(
            sql.Identifier("exchange_history"),
            sql.Identifier(target_table),
        )

        cursor.execute(upsert_query)
        upserted_dates = cursor.rowcount

        logger.info(
            "Hoàn tất exchange=%s, symbols=%s, skipped=%s, "
            "source_rows=%s, trading_dates=%s",
            exchange,
            loaded_symbols,
            skipped_symbols,
            loaded_rows,
            upserted_dates,
        )


def liquidity_history(
    from_date=None,
    to_date=None,
) -> None:
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

        for exchange, symbols in assets_by_exchange.items():
            try:
                logger.info(
                    "Bắt đầu exchange=%s, symbols=%s",
                    exchange,
                    len(symbols),
                )

                aggregate_exchange(
                    connection=connection,
                    exchange=exchange,
                    symbols=symbols,
                    existing_tables=existing_tables,
                    from_date=from_date,
                    to_date=to_date,
                )

                connection.commit()

            except Exception:
                connection.rollback()

                logger.exception(
                    "Lỗi xử lý exchange=%s",
                    exchange,
                )

    except Exception:
        connection.rollback()
        logger.exception("Không thể tạo lịch sử thanh khoản")
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    # Lần đầu: chạy toàn bộ lịch sử.
    liquidity_history()

    # Những lần sau nên chỉ chạy ngày cần cập nhật:
    #
    # liquidity_history(
    #     from_date="2026-07-28",
    #     to_date="2026-07-28",
    # )