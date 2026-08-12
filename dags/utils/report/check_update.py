import pandas as pd
from datetime import datetime
from pytz import timezone
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
)


def check_update(schema: str, **context):
    """
    True  -> schema này còn thiếu dữ liệu quý trước -> chạy update
    False -> schema này đã đủ dữ liệu quý trước -> skip

    index:
        year + quarter

    các schema khác:
        yearReport + lengthReport
    """

    now = datetime.now(
        timezone("Asia/Ho_Chi_Minh")
    )

    current_year = now.year
    current_quarter = ((now.month - 1) // 3) + 1

    if current_quarter == 1:
        target_year = current_year - 1
        target_quarter = 4
    else:
        target_year = current_year
        target_quarter = current_quarter - 1

    print(
        f"🔎 {schema}: kiểm tra "
        f"Q{target_quarter}/{target_year}"
    )

    symbols = pd.read_sql(
        text("""
            SELECT symbol
            FROM info.asset
            WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
              AND type = 'Stock'
            ORDER BY symbol
        """),
        engine,
    )["symbol"].str.upper().tolist()

    missing_symbols = []

    with engine.begin() as conn:
        for symbol in symbols:

            table_exists = conn.execute(
                text("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = :schema
                          AND table_name = :table
                    )
                """),
                {
                    "schema": schema,
                    "table": symbol,
                },
            ).scalar()

            if not table_exists:
                missing_symbols.append(symbol)
                continue

            if schema == "index":
                period_exists = conn.execute(
                    text(f"""
                        SELECT EXISTS (
                            SELECT 1
                            FROM "{schema}"."{symbol}"
                            WHERE "year" = :year
                              AND "quarter" = :quarter
                        )
                    """),
                    {
                        "year": target_year,
                        "quarter": target_quarter,
                    },
                ).scalar()

            else:
                period_exists = conn.execute(
                    text(f"""
                        SELECT EXISTS (
                            SELECT 1
                            FROM "{schema}"."{symbol}"
                            WHERE "yearReport" = :year
                              AND "lengthReport" = :quarter
                        )
                    """),
                    {
                        "year": target_year,
                        "quarter": target_quarter,
                    },
                ).scalar()

            if not period_exists:
                missing_symbols.append(symbol)

    if missing_symbols:
        print(
            f"⚠️ {schema}: còn {len(missing_symbols)} mã thiếu "
            f"Q{target_quarter}/{target_year}"
        )

        print(
            f"📛 Ví dụ mã thiếu: {missing_symbols[:30]}"
        )

        return True

    print(
        f"✅ {schema}: đã đủ "
        f"Q{target_quarter}/{target_year}"
    )

    return False