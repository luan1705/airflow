import pandas as pd
from sqlalchemy import create_engine, text
import logging

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)


def fetch_eps_all():
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM') AND type = 'Stock'"),
        engine
    )['symbol'].tolist()

    print(f"🚀 Bắt đầu xử lý {len(symbols)} symbol...")
    results = []
    for symbol in symbols:
        try:
            df = pd.read_sql(f"""
                SELECT "isa23" FROM income_statement."{symbol}"
                WHERE "isa23" IS NOT NULL
                ORDER BY "yearReport" DESC, "lengthReport" DESC
                LIMIT 1
            """, engine)
            if df.empty:
                continue
            results.append({"symbol": symbol, "eps": float(df.iloc[0]['isa23'])})
        except Exception as e:
            log.error(f"❌ {symbol}: {e}")

    if not results:
        print("⚠️ Không có dữ liệu.")
        return

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO info.asset (symbol, eps)
            VALUES (:symbol, :eps)
            ON CONFLICT (symbol) DO UPDATE SET eps = EXCLUDED.eps
        """), results)

    print(f"✅ Hoàn tất! Đã update EPS cho {len(results)}/{len(symbols)} symbol.")


if __name__ == "__main__":
    fetch_eps_all()