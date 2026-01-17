from sqlalchemy import create_engine, text
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
engine = create_engine(DB_URL, pool_pre_ping=True)

# lưu exchange dạng TEXT trong details.exchange_foreign
EX_LIST = {"HOSE", "HNX", "UPCOM"}

def live_foreign_exchange(exchange: str):
    ex = exchange.strip().upper()
    if ex not in EX_LIST:
        raise ValueError(f"Unknown exchange={ex}")

    # nguồn 1 phút / live bạn đang có
    src = f'exchange_history."foreign_{ex}_1"'
    # đích 1D tách 3 bảng (đang có sẵn)
    dst_1d = f'exchange_history."foreign_{ex}_1D"'

    try:
        with engine.begin() as conn:
            # 1) SUM bằng SQL
            sums = conn.execute(text(f"""
                SELECT
                  COALESCE(SUM("buyVol"),  0)::bigint  AS "buyVol",
                  COALESCE(SUM("sellVol"), 0)::bigint  AS "sellVol",
                  COALESCE(SUM("netVol"),  0)::bigint  AS "netVol",
                  COALESCE(SUM("buyVal"),  0)::bigint  AS "buyVal",
                  COALESCE(SUM("sellVal"), 0)::bigint  AS "sellVal",
                  COALESCE(SUM("netVal"),  0)::bigint  AS "netVal"
                FROM {src}
            """)).mappings().one()

            # 2) lấy ngày mới nhất để ghi 1D
            day = conn.execute(text(f'SELECT (MAX("time"))::date FROM {src}')).scalar()
            if day is None:
                logging.warning(f"⚠️ {src} empty, skip {ex}")
                return

            # 3) UPSERT live vào 1 bảng gom: details.exchange_foreign (exchange = TEXT)
            payload_live = {"exchange": ex, **sums}
            conn.execute(text("""
                INSERT INTO details.exchange_foreign
                  (exchange, "buyVol","sellVol","netVol","buyVal","sellVal","netVal")
                VALUES
                  (:exchange, :buyVol, :sellVol, :netVol, :buyVal, :sellVal, :netVal)
                ON CONFLICT (exchange) DO UPDATE SET
                  "buyVol"   = EXCLUDED."buyVol",
                  "sellVol"  = EXCLUDED."sellVol",
                  "netVol"   = EXCLUDED."netVol",
                  "buyVal"   = EXCLUDED."buyVal",
                  "sellVal"  = EXCLUDED."sellVal",
                  "netVal"   = EXCLUDED."netVal";
            """), payload_live)

            # 4) UPSERT 1D vào bảng tách theo sàn: exchange_history."foreign_{ex}_1D"
            payload_1d = {"time": day, **sums}
            conn.execute(text(f"""
                INSERT INTO {dst_1d}
                  ("time","buyVol","sellVol","netVol","buyVal","sellVal","netVal")
                VALUES
                  (:time,:buyVol,:sellVol,:netVol,:buyVal,:sellVal,:netVal)
                ON CONFLICT ("time") DO UPDATE SET
                  "buyVol"  = EXCLUDED."buyVol",
                  "sellVol" = EXCLUDED."sellVol",
                  "netVol"  = EXCLUDED."netVol",
                  "buyVal"  = EXCLUDED."buyVal",
                  "sellVal" = EXCLUDED."sellVal",
                  "netVal"  = EXCLUDED."netVal";
            """), payload_1d)

            logging.info(f"✅ {ex} OK | day={day} | netVol={sums['netVol']} netVal={sums['netVal']}")

    except Exception:
        logging.exception(f"❌ live_foreign_exchange failed: {ex}")
        raise  # để Airflow retry

def foreign_HOSE():  return live_foreign_exchange("HOSE")
def foreign_HNX():   return live_foreign_exchange("HNX")
def foreign_UPCOM(): return live_foreign_exchange("UPCOM")
