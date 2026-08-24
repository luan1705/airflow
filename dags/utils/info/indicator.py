from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import pandas as pd
import logging
import time

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(DB_URL)


def update_indicator():
    symbols = pd.read_sql(
        text("""
            SELECT symbol
            FROM info.asset
            WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
              AND type = 'Stock'
        """),
        engine
    )["symbol"].tolist()

    print(f"🚀 Bắt đầu xử lý {len(symbols)} symbol...")

    results = []

    for symbol in symbols:
        try:
            df = pd.read_sql(f"""
                SELECT
                    roa,
                    roe,
                    pe,
                    pb,
                    "marketCap",
                    "numberOfSharesMktCap"
                FROM index."{symbol}"
                WHERE "lengthReport" BETWEEN 1 AND 4
                ORDER BY "yearReport" DESC, "lengthReport" DESC
                LIMIT 1
            """, engine)

            if df.empty:
                continue

            row = df.iloc[0]

            roa = None if pd.isna(row["roa"]) or row["roa"] == 0 else float(row["roa"])
            roe = None if pd.isna(row["roe"]) or row["roe"] == 0 else float(row["roe"])
            pe  = None if pd.isna(row["pe"])  or row["pe"]  == 0 else float(row["pe"])
            pb  = None if pd.isna(row["pb"])  or row["pb"]  == 0 else float(row["pb"])

            market_cap = (
                None
                if pd.isna(row["marketCap"])
                else float(row["marketCap"])
            )

            shares_outstanding = (
                None
                if pd.isna(row["numberOfSharesMktCap"])
                else float(row["numberOfSharesMktCap"])
            )

            eps = None

            if (
                market_cap is not None
                and shares_outstanding is not None
                and shares_outstanding != 0
                and pe is not None
                and pe != 0
            ):
                eps = (
                    market_cap
                    / shares_outstanding
                    / pe
                )

            results.append({
                "symbol": symbol,
                "roa": roa,
                "roe": roe,
                "pe": pe,
                "pb": pb,
                "eps": eps,
                "marketCap": market_cap,
                "sharesOutstanding": shares_outstanding,
            })

        except Exception as e:
            log.error(f"❌ {symbol}: {e}")

    if not results:
        print("⚠️ Không có dữ liệu.")
        return

    # Quan trọng: lock theo thứ tự cố định
    results.sort(key=lambda x: x["symbol"])

    sql = text("""
        INSERT INTO info.asset (
            symbol,
            roa,
            roe,
            pe,
            pb,
            eps,
            "marketCap",
            "sharesOutstanding"
        )
        VALUES (
            :symbol,
            :roa,
            :roe,
            :pe,
            :pb,
            :eps,
            :marketCap,
            :sharesOutstanding
        )
        ON CONFLICT (symbol) DO UPDATE SET
            roa = EXCLUDED.roa,
            roe = EXCLUDED.roe,
            pe  = EXCLUDED.pe,
            pb  = EXCLUDED.pb,
            eps = EXCLUDED.eps,
            "marketCap" = EXCLUDED."marketCap",
            "sharesOutstanding" = EXCLUDED."sharesOutstanding"
    """)

    MAX_RETRIES = 3

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with engine.begin() as conn:
                conn.execute(sql, results)

            print(
                f"✅ Hoàn tất! Đã upsert "
                f"{len(results)} symbol trong 1 execute."
            )
            return

        except OperationalError as e:
            if "deadlock detected" not in str(e).lower():
                raise

            if attempt >= MAX_RETRIES:
                log.error(
                    "❌ Deadlock sau %d lần retry",
                    MAX_RETRIES
                )
                raise

            wait_time = 0.5 * attempt

            log.warning(
                "⚠️ Deadlock attempt %d/%d, retry sau %.1fs",
                attempt,
                MAX_RETRIES,
                wait_time
            )

            time.sleep(wait_time)
            
if __name__ == "__main__":
    update_indicator()