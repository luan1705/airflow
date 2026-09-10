import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from db_config import POSTGRES_URL

engine = create_engine(POSTGRES_URL, poolclass=NullPool)


def sync_tradingview():
    df = pd.read_sql(
        text("SELECT symbol, exchange, name, type FROM info.asset WHERE symbol IS NOT NULL"),
        engine
    )

    if df.empty:
        print("⚠️ Không có dữ liệu.")
        return

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO info.tradingview (symbol, exchange, name, type)
            VALUES (:symbol, :exchange, :name, :type)
            ON CONFLICT (symbol) DO UPDATE SET
                exchange = EXCLUDED.exchange,
                name     = EXCLUDED.name,
                type     = EXCLUDED.type
        """), df.to_dict(orient="records"))

    print(f"✅ Hoàn tất! Đã upsert {len(df)} rows vào info.tradingview.")


if __name__ == "__main__":
    sync_tradingview()