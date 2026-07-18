from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"


# ✅ Nếu DB hay báo "too many clients already" thì NullPool là an toàn nhất trong Airflow
# (mỗi task mở 1 connection rồi đóng ngay, không giữ connection trong pool)
def get_engine():
    return create_engine(
        DB_URL,
        pool_pre_ping=True,
        poolclass=NullPool,
    )


EX_CFG = {
    "HOSE":  {"index_symbol": "VNINDEX",    "exchange_id": 1},
    "HNX":   {"index_symbol": "HNXINDEX",   "exchange_id": 2},
    "UPCOM": {"index_symbol": "UPCOMINDEX", "exchange_id": 3},
}

IDX_SQL = text("""
    SELECT "point","advancers","noChanges","decliners","totalVal","totalVol","advancersVal", "noChangesVal", "declinersVal"
    FROM indices.vietnam
    WHERE symbol = :symbol
    LIMIT 1
""")

UPSERT_SQL = text("""
    INSERT INTO details.exchange_volatility
        (exchange, advancers, "noChanges", decliners,
         "advancersVal", "noChangesVal", "declinersVal",
         "totalVol", "totalVal", point)
    VALUES
        (:exchange, :advancers, :noChanges, :decliners,
         :advancersVal, :noChangesVal, :declinersVal,
         :totalVol, :totalVal, :point)
    ON CONFLICT (exchange) DO UPDATE SET
        advancers      = EXCLUDED.advancers,
        "noChanges"     = EXCLUDED."noChanges",
        decliners      = EXCLUDED.decliners,
        "advancersVal" = EXCLUDED."advancersVal",
        "noChangesVal"  = EXCLUDED."noChangesVal",
        "declinersVal" = EXCLUDED."declinersVal",
        "totalVol"     = EXCLUDED."totalVol",
        "totalVal"     = EXCLUDED."totalVal",
        point          = EXCLUDED.point
""")


# def _get_stock_exchange_type(conn) -> str:
#     """
#     Trả về data_type của cột info.asset.exchange trong information_schema.
#     Ví dụ: 'text', 'character varying', 'bigint', 'integer', ...
#     """
#     t = conn.execute(text("""
#         SELECT data_type
#         FROM information_schema.columns
#         WHERE table_schema = 'info'
#           AND table_name   = 'asset'
#           AND column_name  = 'exchange'
#         LIMIT 1
#     """)).scalar()

#     return (t or "").lower().strip()


# def _build_val_sql():
#     return text("""
#         SELECT
#           COALESCE(SUM(CASE WHEN e."matchChange" > 0 THEN e."totalVal" ELSE 0 END), 0) AS "advancersVal",
#           COALESCE(SUM(CASE WHEN e."matchChange" = 0 THEN e."totalVal" ELSE 0 END), 0) AS "noChangesVal",
#           COALESCE(SUM(CASE WHEN e."matchChange" < 0 THEN e."totalVal" ELSE 0 END), 0) AS "declinersVal"
#         FROM details.asset e
#         JOIN info.asset s
#           ON s.symbol = e.symbol
#         WHERE s.indices LIKE '%' || :index_symbol || '%'
#           AND s.available = TRUE
#           AND s.active = TRUE
#     """)


def upsert_exchange_volatility(exchange: str):
    exchange = exchange.strip().upper()
    cfg = EX_CFG.get(exchange)
    if not cfg:
        raise ValueError(f"Unsupported exchange: {exchange}")

    index_symbol = cfg["index_symbol"]
    exchange_id = cfg["exchange_id"]

    logging.info("Start exchange_volatility: %s (%s)", exchange, index_symbol)

    engine = get_engine()
    try:
        with engine.begin() as conn:
            # 0) Detect kiểu cột info.stock.exchange để tránh lỗi text = integer
            # val_sql = _build_val_sql()

            # params = {"index_symbol": index_symbol}

            # # 1) Tính advancersVal/noChangesVal/declinersVal
            # val = pd.read_sql(val_sql, con=conn, params=params).iloc[0].to_dict()

            # 2) Lấy dữ liệu index
            idx = pd.read_sql(IDX_SQL, con=conn, params={"symbol": index_symbol})
            if idx.empty:
                logging.warning("⚠️ Missing index row: %s", index_symbol)
                return

            i = idx.iloc[0]
            row = {
                # PK của details.exchange_volatility là TEXT (HOSE/HNX/UPCOM)
                "exchange": exchange,
                "point": float(i["point"] or 0),
                "advancers": int(i["advancers"] or 0),
                "noChanges": int(i["noChanges"] or 0),
                "decliners": int(i["decliners"] or 0),
                "totalVal": float(i["totalVal"] or 0),
                "totalVol": float(i["totalVol"] or 0),
                "advancersVal": float(i["advancersVal"] or 0),
                "noChangesVal": float(i["noChangesVal"] or 0),
                "declinersVal": float(i["declinersVal"] or 0),
            }

            # 3) Upsert
            conn.execute(UPSERT_SQL, row)

        logging.info("✅ Upsert OK: details.exchange_volatility (%s)", exchange)

    except Exception:
        logging.exception("❌ exchange_volatility failed: %s", exchange)
        raise  # ✅ để Airflow retry
    finally:
        engine.dispose()


# Wrappers để Airflow gọi như cũ
def volatility_HOSE():
    return upsert_exchange_volatility("HOSE")


def volatility_HNX():
    return upsert_exchange_volatility("HNX")


def volatility_UPCOM():
    return upsert_exchange_volatility("UPCOM")
