from sqlalchemy import create_engine, text
import pandas as pd
import logging

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)


def update_indicator():
    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM') AND type = 'Stock'"),
        engine
    )['symbol'].tolist()

    print(f"🚀 Bắt đầu xử lý {len(symbols)} symbol...")
    results = []
    for symbol in symbols:
        try:
            df = pd.read_sql(f"""
                SELECT roa, roe, pe, pb, "marketCap", "numberOfSharesMktCap"
                FROM index."{symbol}"
                WHERE quarter BETWEEN 1 AND 4
                ORDER BY year DESC, quarter DESC
                LIMIT 1
            """, engine)
            if df.empty:
                continue
            row = df.iloc[0]
            results.append({
                "symbol":            symbol,
                "roa":               None if not row['roa'] else float(row['roa']),
                "roe":               None if not row['roe'] else float(row['roe']),
                "pe":                None if not row['pe'] else float(row['pe']),
                "pb":                None if not row['pb'] else float(row['pb']),
                "marketCap":         row['marketCap'],
                "sharesOutstanding": row['numberOfSharesMktCap'],
            })
        except Exception as e:
            log.error(f"❌ {symbol}: {e}")

    if not results:
        print("⚠️ Không có dữ liệu.")
        return

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO info.asset (symbol, roa, roe, pe, pb, "marketCap", "sharesOutstanding")
            VALUES (:symbol, :roa, :roe, :pe, :pb, :marketCap, :sharesOutstanding)
            ON CONFLICT (symbol) DO UPDATE SET
                roa = EXCLUDED.roa,
                roe = EXCLUDED.roe,
                pe  = EXCLUDED.pe,
                pb  = EXCLUDED.pb,
                "marketCap"         = EXCLUDED."marketCap",
                "sharesOutstanding" = EXCLUDED."sharesOutstanding"
        """), results)

    print(f"✅ Hoàn tất! Đã upsert {len(results)} symbol.")


if __name__ == "__main__":
    update_indicator()