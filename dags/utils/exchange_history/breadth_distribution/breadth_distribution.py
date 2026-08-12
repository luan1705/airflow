import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from zoneinfo import ZoneInfo

# =========================================================
# CONFIG
# =========================================================

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"

SCHEMA = "exchange_history"

EXCHANGES = (
    "HOSE",
    "HNX",
    "UPCOM",
)

MAX_WORKERS = 15

log = logging.getLogger(__name__)

engine = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


# =========================================================
# BUCKET CONFIG
# =========================================================

BUCKET_LABELS = (
    "<= -7%",
    "(-7%, -5%]",
    "(-5%, -3%]",
    "(-3%, -1%]",
    "(-1%, 0%)",
    "0%",
    "(0%, 1%)",
    "[1%, 3%)",
    "[3%, 5%)",
    "[5%, 7%)",
    ">= 7%",
)


def create_empty_distribution() -> dict[str, int]:
    return {
        label: 0
        for label in BUCKET_LABELS
    }


def classify_change(pct_change: float) -> str:
    """
    Phân loại phần trăm thay đổi vào 11 khoảng.

    Quy tắc:
    - <= -7%
    - (-7%, -5%]
    - (-5%, -3%]
    - (-3%, -1%]
    - (-1%, 0%)
    - 0%
    - (0%, 1%)
    - [1%, 3%)
    - [3%, 5%)
    - [5%, 7%)
    - >= 7%
    """

    if abs(pct_change) < 0.000001:
        return "0%"

    if pct_change <= -7:
        return "<= -7%"

    if pct_change <= -5:
        return "(-7%, -5%]"

    if pct_change <= -3:
        return "(-5%, -3%]"

    if pct_change <= -1:
        return "(-3%, -1%]"

    if pct_change < 0:
        return "(-1%, 0%)"

    if pct_change < 1:
        return "(0%, 1%)"

    if pct_change < 3:
        return "[1%, 3%)"

    if pct_change < 5:
        return "[3%, 5%)"

    if pct_change < 7:
        return "[5%, 7%)"

    return ">= 7%"


# =========================================================
# TABLE NAME
# =========================================================

def get_table_name(exchange: str) -> str:
    """
    HOSE  -> bread_distribution_HOSE
    HNX   -> bread_distribution_HNX
    UPCOM -> bread_distribution_UPCOM
    """

    if exchange not in EXCHANGES:
        raise ValueError(
            f"Exchange không hợp lệ: {exchange}"
        )

    return f"bread_distribution_{exchange}"


# =========================================================
# VNINDEX ANCHOR
# =========================================================

def has_vnindex_data(
    target_date: date,
) -> bool:
    """
    Kiểm tra VNINDEX đã có dữ liệu OHLCV trong ngày cần tính hay chưa.

    Nếu chưa có:
    - Không tính breadth.
    - Không cập nhật dữ liệu ngày hôm qua.
    - Không ghi giá hôm qua vào ngày hôm nay.
    """

    sql = text("""
        SELECT EXISTS (
            SELECT 1
            FROM ohlcv."VNINDEX_1D"
            WHERE time::date = :target_date
              AND close IS NOT NULL
              AND close > 0
        )
    """)

    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "target_date": target_date,
            },
        ).scalar()

    return bool(result)


def get_vnindex_peak(target_date: date) -> date:
    """
    Tìm ngày VNINDEX có close cao nhất trong 6 tháng
    tính đến target_date.

    Trả về ngày dùng làm anchor_time cho toàn bộ cổ phiếu.
    """

    sql = text("""
        SELECT time::date AS anchor_time
        FROM ohlcv."VNINDEX_1D"
        WHERE time::date <= :target_date
          AND time::date >= (
              CAST(:target_date AS DATE)
              - INTERVAL '6 months'
          )
          AND close IS NOT NULL
          AND close > 0
        ORDER BY
            close DESC,
            time DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(
            sql,
            {
                "target_date": target_date,
            },
        ).mappings().first()

    if row is None:
        raise ValueError(
            f"Không tìm thấy dữ liệu VNINDEX trong 6 tháng "
            f"tính đến {target_date}"
        )

    return row["anchor_time"]


# =========================================================
# SYMBOL LIST
# =========================================================

def get_symbols_by_exchange(
    exchange: str,
) -> list[str]:
    """
    Lấy danh sách mã cổ phiếu thuộc một sàn.
    """

    if exchange not in EXCHANGES:
        raise ValueError(
            f"Exchange không hợp lệ: {exchange}"
        )

    sql = text("""
        SELECT DISTINCT symbol
        FROM info.asset
        WHERE exchange = :exchange
          AND symbol IS NOT NULL
          AND TRIM(symbol) <> ''
        ORDER BY symbol
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "exchange": exchange,
            },
        ).fetchall()

    return [
        str(row[0]).strip()
        for row in rows
    ]


# =========================================================
# STOCK PRICES
# =========================================================

def get_symbol_prices(
    symbol: str,
    anchor_time: date,
    target_date: date,
) -> dict | None: 
    """
    Lấy hai mức giá của một cổ phiếu:

    1. anchor_close:
       Giá đóng cửa đúng ngày VNINDEX đạt close cao nhất
       trong 6 tháng.

    2. current_close:
       Giá đóng cửa đúng ngày target_date.

    Nếu không có giá đúng anchor_time thì bỏ mã.
    """

    table_name = f"{symbol}_1D"

    sql = text(f"""
        WITH anchor_price AS (
            SELECT close
            FROM ohlcv."{table_name}"
            WHERE time::date = :anchor_time
              AND close IS NOT NULL
              AND close > 0
            ORDER BY time DESC
            LIMIT 1
        ),
        current_price AS (
            SELECT close
            FROM ohlcv."{table_name}"
            WHERE time::date = :target_date
              AND close IS NOT NULL
              AND close > 0
            ORDER BY time DESC
            LIMIT 1
        )
        SELECT
            anchor_price.close AS anchor_close,
            current_price.close AS current_close
        FROM anchor_price
        CROSS JOIN current_price
    """)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                sql,
                {
                    "anchor_time": anchor_time,
                    "target_date": target_date,
                },
            ).mappings().first()

        if row is None:
            return None

        anchor_close = float(row["anchor_close"])
        current_close = float(row["current_close"])

        if anchor_close <= 0 or current_close <= 0:
            return None

        return {
            "symbol": symbol,
            "anchor_close": anchor_close,
            "current_close": current_close,
        }

    except Exception as exc:
        # Các trường hợp thường gặp:
        # - Không tồn tại bảng ohlcv."SYMBOL_1D"
        # - Bảng thiếu dữ liệu
        log.debug(
            "Không lấy được dữ liệu mã %s: %s",
            symbol,
            exc,
        )
        return None


def load_symbol_prices(
    symbols: list[str],
    anchor_time: date,
    target_date: date,
) -> list[dict]:
    """
    Đọc giá cổ phiếu song song bằng ThreadPoolExecutor.
    """

    results: list[dict] = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS,
    ) as executor:

        futures = {
            executor.submit(
                get_symbol_prices,
                symbol,
                anchor_time,
                target_date,
            ): symbol
            for symbol in symbols
        }

        for future in as_completed(futures):
            symbol = futures[future]

            try:
                item = future.result()

                if item is not None:
                    results.append(item)

            except Exception as exc:
                log.warning(
                    "Lỗi xử lý mã %s: %s",
                    symbol,
                    exc,
                )

    return results


# =========================================================
# CALCULATE DISTRIBUTION
# =========================================================

def calculate_distribution(
    stock_data: list[dict],
) -> dict[str, int]:
    """
    Tính mức thay đổi của từng cổ phiếu:

        pct_change =
            (current_close / anchor_close - 1) * 100

    Sau đó đếm số lượng mã trong từng khoảng.
    """

    distribution = create_empty_distribution()

    for item in stock_data:
        anchor_close = item["anchor_close"]
        current_close = item["current_close"]

        pct_change = (
            current_close / anchor_close - 1
        ) * 100

        bucket = classify_change(pct_change)

        distribution[bucket] += 1

    return distribution


# =========================================================
# DATABASE TABLE
# =========================================================

def ensure_distribution_table(
    exchange: str,
) -> None:
    """
    Tạo bảng cho một sàn nếu chưa tồn tại.

    Kết quả chỉ gồm:
    - time
    - các khoảng decliner
    - unchanged
    - các khoảng advancer
    """

    table_name = get_table_name(exchange)

    with engine.begin() as conn:
        conn.execute(text(
            f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'
        ))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS
            "{SCHEMA}"."{table_name}" (
                time DATE PRIMARY KEY,

                decliner_7
                    INTEGER NOT NULL DEFAULT 0,

                decliner_7_5
                    INTEGER NOT NULL DEFAULT 0,

                decliner_5_3
                    INTEGER NOT NULL DEFAULT 0,

                decliner_3_1
                    INTEGER NOT NULL DEFAULT 0,

                decliner_1_0
                    INTEGER NOT NULL DEFAULT 0,

                unchanged
                    INTEGER NOT NULL DEFAULT 0,

                advancer_0_1
                    INTEGER NOT NULL DEFAULT 0,

                advancer_1_3
                    INTEGER NOT NULL DEFAULT 0,

                advancer_3_5
                    INTEGER NOT NULL DEFAULT 0,

                advancer_5_7
                    INTEGER NOT NULL DEFAULT 0,

                advancer_7
                    INTEGER NOT NULL DEFAULT 0
            )
        """))


def save_distribution(
    exchange: str,
    target_date: date,
    distribution: dict[str, int],
) -> None:
    """
    Insert hoặc update kết quả theo ngày.
    """

    table_name = get_table_name(exchange)

    params = {
        "time": target_date,

        # Decliner
        "decliner_7": distribution["<= -7%"],
        "decliner_7_5": distribution["(-7%, -5%]"],
        "decliner_5_3": distribution["(-5%, -3%]"],
        "decliner_3_1": distribution["(-3%, -1%]"],
        "decliner_1_0": distribution["(-1%, 0%)"],

        # Không đổi
        "unchanged": distribution["0%"],

        # Advancer
        "advancer_0_1": distribution["(0%, 1%)"],
        "advancer_1_3": distribution["[1%, 3%)"],
        "advancer_3_5": distribution["[3%, 5%)"],
        "advancer_5_7": distribution["[5%, 7%)"],
        "advancer_7": distribution[">= 7%"],
    }

    sql = text(f"""
        INSERT INTO "{SCHEMA}"."{table_name}" (
            time,

            decliner_7,
            decliner_7_5,
            decliner_5_3,
            decliner_3_1,
            decliner_1_0,

            unchanged,

            advancer_0_1,
            advancer_1_3,
            advancer_3_5,
            advancer_5_7,
            advancer_7
        )
        VALUES (
            :time,

            :decliner_7,
            :decliner_7_5,
            :decliner_5_3,
            :decliner_3_1,
            :decliner_1_0,

            :unchanged,

            :advancer_0_1,
            :advancer_1_3,
            :advancer_3_5,
            :advancer_5_7,
            :advancer_7
        )
        ON CONFLICT (time) DO UPDATE SET
            decliner_7 =
                EXCLUDED.decliner_7,

            decliner_7_5 =
                EXCLUDED.decliner_7_5,

            decliner_5_3 =
                EXCLUDED.decliner_5_3,

            decliner_3_1 =
                EXCLUDED.decliner_3_1,

            decliner_1_0 =
                EXCLUDED.decliner_1_0,

            unchanged =
                EXCLUDED.unchanged,

            advancer_0_1 =
                EXCLUDED.advancer_0_1,

            advancer_1_3 =
                EXCLUDED.advancer_1_3,

            advancer_3_5 =
                EXCLUDED.advancer_3_5,

            advancer_5_7 =
                EXCLUDED.advancer_5_7,

            advancer_7 =
                EXCLUDED.advancer_7
    """)

    with engine.begin() as conn:
        conn.execute(
            sql,
            params,
        )


# =========================================================
# PROCESS ONE EXCHANGE
# =========================================================

def process_exchange(
    exchange: str,
    target_date: date,
    anchor_time: date,
) -> dict:
    """
    Xử lý một sàn:
    - Lấy danh sách mã.
    - Lấy anchor close và current close.
    - Tính phân phối.
    - Lưu vào bảng tương ứng.
    """

    log.info(
        "Bắt đầu xử lý %s",
        exchange,
    )

    symbols = get_symbols_by_exchange(exchange)

    log.info(
        "%s có %s mã trong info.asset",
        exchange,
        len(symbols),
    )

    stock_data = load_symbol_prices(
        symbols=symbols,
        anchor_time=anchor_time,
        target_date=target_date,
    )

    log.info(
        "%s có %s/%s mã đủ dữ liệu",
        exchange,
        len(stock_data),
        len(symbols),
    )

    distribution = calculate_distribution(
        stock_data
    )

    ensure_distribution_table(exchange)

    save_distribution(
        exchange=exchange,
        target_date=target_date,
        distribution=distribution,
    )

    result = {
        "time": target_date.isoformat(),
        "decliner_7": distribution["<= -7%"],
        "decliner_7_5": distribution["(-7%, -5%]"],
        "decliner_5_3": distribution["(-5%, -3%]"],
        "decliner_3_1": distribution["(-3%, -1%]"],
        "decliner_1_0": distribution["(-1%, 0%)"],
        "unchanged": distribution["0%"],
        "advancer_0_1": distribution["(0%, 1%)"],
        "advancer_1_3": distribution["[1%, 3%)"],
        "advancer_3_5": distribution["[3%, 5%)"],
        "advancer_5_7": distribution["[5%, 7%)"],
        "advancer_7": distribution[">= 7%"],
    }

    log.info(
        "Hoàn thành %s | số mã được tính: %s",
        exchange,
        len(stock_data),
    )

    return result


# =========================================================
# MAIN
# =========================================================

def breadth_distribution(
    **context,
) -> dict:
    """
    Tính breadth realtime cho ngày hiện tại theo giờ Việt Nam.

    Logic:
    - Lấy ngày hiện tại theo Asia/Ho_Chi_Minh.
    - Nếu VNINDEX chưa có dữ liệu hôm nay thì bỏ qua.
    - Tìm ngày VNINDEX có close cao nhất trong 6 tháng.
    - Lấy giá từng cổ phiếu đúng ngày hôm nay.
    - Tính phân phối cho HOSE, HNX và UPCOM.
    - Upsert vào dòng của ngày hôm nay.

    Trong ngày:
    - Các lần chạy tiếp theo chỉ cập nhật dòng hôm nay.

    Sang ngày mới:
    - Tạo dòng mới.
    - Không ảnh hưởng dữ liệu ngày hôm trước.
    """

    target_date = datetime.now(VN_TZ).date()

    log.info(
        "Bắt đầu tính breadth distribution ngày %s",
        target_date,
    )

    if not has_vnindex_data(target_date):
        log.info(
            "VNINDEX chưa có dữ liệu ngày %s, bỏ qua lần chạy",
            target_date,
        )

        return {
            "status": "skipped",
            "reason": "vnindex_not_available",
            "time": target_date.isoformat(),
        }

    anchor_time = get_vnindex_peak(target_date)

    log.info(
        "Ngày tính: %s | ngày VNINDEX đạt đỉnh 6 tháng: %s",
        target_date,
        anchor_time,
    )

    results = {}

    for exchange in EXCHANGES:
        try:
            results[exchange] = process_exchange(
                exchange=exchange,
                target_date=target_date,
                anchor_time=anchor_time,
            )

        except Exception:
            log.exception(
                "Lỗi xử lý sàn %s",
                exchange,
            )
            raise

    log.info(
        "Hoàn thành HOSE, HNX và UPCOM ngày %s",
        target_date,
    )

    return results


# =========================================================
# RUN DIRECTLY
# =========================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )

    result = breadth_distribution()

    for exchange, data in result.items():
        print(exchange, data)