from sqlalchemy import create_engine
import pandas as pd
import logging
import numpy as np
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"

def sector():
    enginedb = None
    try:
        enginedb = create_engine(DB_URL, pool_pre_ping=True)
        logging.info("Kết nối DB")

        # === Lấy dữ liệu gốc ===
        df0 = pd.read_sql("""
            SELECT *
            FROM "info"."asset" left join "details"."asset" using("symbol")
        """, con=enginedb)

        # Ép numeric để tính toán không bị object
        for c in [
            "totalVal","foreignBuyVal","foreignSellVal","marketCap",
            "pe","pb","matchRatioChange","open","close","refPrice"
        ]:
            if c in df0.columns:
                df0[c] = pd.to_numeric(df0[c], errors="coerce")

        # =========================
        # A) TREND COUNT (tách riêng, chỉ dropna cho trend)
        # =========================
        df_trend = df0.dropna(subset=["open", "refPrice", "close", "sector"]).copy()

        df_trend["advancers"] = (df_trend["close"] > df_trend["refPrice"]).astype(np.int32)
        df_trend["decliners"] = (df_trend["close"] < df_trend["refPrice"]).astype(np.int32)
        df_trend["noChanges"] = (df_trend["close"] == df_trend["refPrice"]).astype(np.int32)

        trend_count = (
            df_trend.groupby("sector", as_index=False)[["advancers","decliners","noChanges"]]
            .sum()
        )

        # =========================
        # B) CHỈ SỐ NGÀNH (KHÔNG drop theo open/refPrice để khỏi mất marketCap)
        # =========================
        df = df0.dropna(subset=["sector"]).copy()

        df["foreignNetVal"] = df["foreignBuyVal"].fillna(0) - df["foreignSellVal"].fillna(0)

        mcap = df["marketCap"].fillna(0)
        pe = df["pe"]
        pb = df["pb"]

        # chia an toàn tránh inf
        df["profit"] = np.where(pe > 0, mcap / pe, np.nan)
        df["equity"] = np.where(pb > 0, mcap / pb, np.nan)

        # marketcap weigh theo ngành (tránh chia 0)
        df["totalMarketCap_sector"] = df.groupby("sector")["marketCap"].transform("sum").fillna(0)
        df["marketCapweigh"] = np.where(df["totalMarketCap_sector"] > 0, mcap / df["totalMarketCap_sector"], 0.0)

        # weighted matchRatioChange
        df["matchRatioChange_w"] = df["matchRatioChange"].fillna(0) * df["marketCapweigh"]

        # replace inf nếu có (phòng hờ) rồi group
        df = df.replace([np.inf, -np.inf], np.nan)

        # group ngành (tính luôn profit/equity trong agg để tránh lỗi ndim=2 do trùng tên cột)
        df_clean = (
            df.groupby("sector", as_index=False)
              .agg(
                  totalVal=("totalVal", "sum"),
                  foreignNetVal=("foreignNetVal", "sum"),
                  marketCap=("marketCap", "sum"),
                  matchRatioChange=("matchRatioChange_w", "sum"),
                  profit=("profit", lambda s: s.sum(min_count=1)),
                  equity=("equity", lambda s: s.sum(min_count=1)),
              )
        )

        # tính lại pe/pb ngành
        df_clean["pe"] = np.where(df_clean["profit"] > 0, df_clean["marketCap"] / df_clean["profit"], 0.0)
        df_clean["pb"] = np.where(df_clean["equity"] > 0, df_clean["marketCap"] / df_clean["equity"], 0.0)

        # weight
        total_marketcap = df_clean["marketCap"].sum()
        df_clean["weight"] = np.where(total_marketcap > 0, df_clean["marketCap"] / total_marketcap * 100, 0.0)

        # =========================
        # C) MERGE + ORDER CỘT như bạn gửi
        # =========================
        df_final = df_clean.merge(trend_count, on="sector", how="left").fillna(0)

        df_final[["advancers","decliners","noChanges"]] = df_final[["advancers","decliners","noChanges"]].astype(np.int32)

        cols_order = [
            "sector",
            "totalVal",
            "foreignNetVal",
            "marketCap",
            "pe",
            "pb",
            "matchRatioChange",
            "weight",
            "advancers",
            "decliners",
            "noChanges",
        ]
        df_final = df_final.reindex(columns=cols_order)

        # === Ghi DB ===
        cols = cols_order
        col_list = ', '.join(f'"{c}"' for c in cols)
        update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'sector')
        rows_data = [tuple(r) for r in df_final.itertuples(index=False)]

        with enginedb.begin() as conn:
            with conn.connection.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                        INSERT INTO details.sector ({col_list})
                        VALUES %s
                        ON CONFLICT ("sector") DO UPDATE SET {update_set}
                    """,
                    rows_data,
                    page_size=1000
                )

        logging.info("Đã lưu sector với trend count và weight")

    except Exception:
        logging.exception("Lỗi lưu sector")
        raise
    finally:
        if enginedb is not None:
            enginedb.dispose()
            logging.info("🔌 Đã đóng kết nối DB")
