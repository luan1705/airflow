import pandas as pd
from sqlalchemy import create_engine, text
import logging
from sqlalchemy.pool import NullPool
from db_config import POSTGRES_URL

log = logging.getLogger(__name__)

engine = create_engine(POSTGRES_URL, poolclass=NullPool)


def update_market_weight():
    df = pd.read_sql(
        text("""
            SELECT symbol, "sectorGroup", "marketCap"
            FROM info.asset
            WHERE "marketCap" IS NOT NULL AND "sectorGroup" IS NOT NULL
        """),
        engine
    )

    if df.empty:
        print("⚠️ Không có dữ liệu.")
        return

    total_market_cap = df['marketCap'].sum()
    df['marketWeight'] = df['marketCap'] / total_market_cap

    sector_total = df.groupby('sectorGroup')['marketCap'].sum().rename('sectorTotal')
    df = df.join(sector_total, on='sectorGroup')
    df['sectorGroupWeight'] = df['marketCap'] / df['sectorTotal']

    results = df[['symbol', 'marketWeight', 'sectorGroupWeight']].to_dict(orient='records')

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO info.asset (symbol, "marketWeight", "sectorGroupWeight")
            VALUES (:symbol, :marketWeight, :sectorGroupWeight)
            ON CONFLICT (symbol) DO UPDATE SET
                "marketWeight"      = EXCLUDED."marketWeight",
                "sectorGroupWeight" = EXCLUDED."sectorGroupWeight"
        """), results)

    print(f"✅ Hoàn tất! Đã update {len(results)} symbol.")


if __name__ == "__main__":
    update_market_weight()