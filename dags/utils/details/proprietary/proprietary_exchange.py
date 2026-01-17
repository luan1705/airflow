from sqlalchemy import create_engine, text
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"

# dùng 1 engine chung (đỡ tạo nhiều connection)
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_size=2,
    max_overflow=2,
)

def upsert_exchange_proprietary(exchange: str):
    ex = exchange.strip().upper()
    src = f'exchange_history."proprietary_{ex}_1D"'

    try:
        with engine.begin() as conn:
            # 1) lấy ngày mới nhất
            day = conn.execute(text(f'SELECT MAX("time")::date FROM {src}')).scalar()
            if day is None:
                logging.warning(f"⚠️ {src} empty -> skip {ex}")
                return

            # 2) lấy số liệu của ngày đó
            sums = conn.execute(
                text(f"""
                    SELECT
                      COALESCE(SUM("netMatchVal"), 0)::double precision AS "netMatchVal",
                      COALESCE(SUM("netDealVal"),  0)::double precision AS "netDealVal",
                      COALESCE(SUM("netMatchVol"), 0)::double precision AS "netMatchVol",
                      COALESCE(SUM("netDealVol"),  0)::double precision AS "netDealVol"
                    FROM {src}
                    WHERE "time"::date = :day
                """),
                {"day": day},
            ).mappings().one()

            netVal = float(sums["netMatchVal"] + sums["netDealVal"])
            netVol = float(sums["netMatchVol"] + sums["netDealVol"])

            payload = {
                "exchange": ex,   # lưu tên sàn như hình bạn nói
                "date": day,
                "netMatchVal": float(sums["netMatchVal"]),
                "netDealVal":  float(sums["netDealVal"]),
                "netVal":      netVal,
                "netMatchVol": float(sums["netMatchVol"]),
                "netDealVol":  float(sums["netDealVol"]),
                "netVol":      netVol,
            }

            # 3) upsert vào 1 bảng duy nhất
            conn.execute(text("""
                INSERT INTO details.exchange_proprietary
                  (exchange, date, "netMatchVal","netDealVal","netVal","netMatchVol","netDealVol","netVol")
                VALUES
                  (:exchange, :date, :netMatchVal, :netDealVal, :netVal, :netMatchVol, :netDealVol, :netVol)
                ON CONFLICT (exchange) DO UPDATE SET
                  date          = EXCLUDED.date,
                  "netMatchVal" = EXCLUDED."netMatchVal",
                  "netDealVal"  = EXCLUDED."netDealVal",
                  "netVal"      = EXCLUDED."netVal",
                  "netMatchVol" = EXCLUDED."netMatchVol",
                  "netDealVol"  = EXCLUDED."netDealVol",
                  "netVol"      = EXCLUDED."netVol";
            """), payload)

            logging.info(f"✅ Upsert details.exchange_proprietary OK: {ex} date={day}")

    except Exception:
        logging.exception(f"❌ upsert_exchange_proprietary failed: {ex}")
        raise  # để Airflow retry

# wrappers cho Airflow nếu bạn vẫn muốn 3 task riêng
def proprietary_HOSE(): return upsert_exchange_proprietary("HOSE")
def proprietary_HNX():  return upsert_exchange_proprietary("HNX")
def proprietary_UPCOM():return upsert_exchange_proprietary("UPCOM")

