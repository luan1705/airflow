from sqlalchemy import create_engine, text
import pandas as pd
import logging
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
engine = create_engine(DB_URL)


def last_trading_day():
    today = datetime.now().date()
    delta = 1
    while True:
        d = today - timedelta(days=delta)
        if d.weekday() < 5:
            return d
        delta += 1


def update_proprietary():
    target_date = last_trading_day()

    symbols = pd.read_sql(
        text("SELECT symbol FROM info.asset WHERE exchange IN ('HOSE', 'HNX', 'UPCOM') AND type = 'Stock'"),
        engine
    )['symbol'].tolist()

    print(f"🚀 Bắt đầu xử lý {len(symbols)} symbol (ngày {target_date})...")
    results = []
    for symbol in symbols:
        try:
            df = pd.read_sql(f"""
                SELECT "netVol", "netVal"
                FROM asset_proprietary_history."{symbol}_1D"
                WHERE DATE(time) = '{target_date}'
                LIMIT 1
            """, engine)
            row = df.iloc[0] if not df.empty else None
            results.append({
                "symbol":            symbol,
                "proprietaryNetVol": float(row['netVol']) if row is not None and pd.notna(row['netVol']) else None,
                "proprietaryNetVal": float(row['netVal']) if row is not None and pd.notna(row['netVal']) else None,
            })
        except Exception as e:
            log.error(f"❌ {symbol}: {e}")

    if not results:
        print("⚠️ Không có dữ liệu.")
        return

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO details.asset (symbol, "proprietaryNetVol", "proprietaryNetVal")
            VALUES (:symbol, :proprietaryNetVol, :proprietaryNetVal)
            ON CONFLICT (symbol) DO UPDATE SET
                "proprietaryNetVol" = EXCLUDED."proprietaryNetVol",
                "proprietaryNetVal" = EXCLUDED."proprietaryNetVal"
        """), results)

    print(f"✅ Hoàn tất! Đã update {len(results)} symbol (ngày {target_date}).")


if __name__ == "__main__":
    update_proprietary()