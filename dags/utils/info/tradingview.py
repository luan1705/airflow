import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)


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