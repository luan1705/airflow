from sqlalchemy import create_engine
import pandas as pd
import logging
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"

def industry():
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
        df_trend = df0.dropna(subset=["open", "refPrice", "close", "industry"]).copy()

        df_trend["advancers"] = (df_trend["close"] > df_trend["refPrice"]).astype(np.int32)
        df_trend["decliners"] = (df_trend["close"] < df_trend["refPrice"]).astype(np.int32)
        df_trend["noChanges"] = (df_trend["close"] == df_trend["refPrice"]).astype(np.int32)

        trend_count = (
            df_trend.groupby("industry", as_index=False)[["advancers","decliners","noChanges"]]
            .sum()
        )

        # =========================
        # B) CHỈ SỐ NGÀNH (KHÔNG drop theo open/refPrice để khỏi mất marketCap)
        # =========================
        df = df0.dropna(subset=["industry"]).copy()

        df["foreignNetVal"] = df["foreignBuyVal"].fillna(0) - df["foreignSellVal"].fillna(0)

        mcap = df["marketCap"].fillna(0)
        pe = df["pe"]
        pb = df["pb"]

        # chia an toàn tránh inf
        df["profit"] = np.where(pe > 0, mcap / pe, np.nan)
        df["equity"] = np.where(pb > 0, mcap / pb, np.nan)

        # marketcap weigh theo ngành (tránh chia 0)
        df["totalMarketCap_industry"] = df.groupby("industry")["marketCap"].transform("sum").fillna(0)
        df["marketCapweigh"] = np.where(df["totalMarketCap_industry"] > 0, mcap / df["totalMarketCap_industry"], 0.0)

        # weighted matchRatioChange
        df["matchRatioChange_w"] = df["matchRatioChange"].fillna(0) * df["marketCapweigh"]

        # replace inf nếu có (phòng hờ) rồi group
        df = df.replace([np.inf, -np.inf], np.nan)

        # group ngành (tính luôn profit/equity trong agg để tránh lỗi ndim=2 do trùng tên cột)
        df_clean = (
            df.groupby("industry", as_index=False)
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
        df_final = df_clean.merge(trend_count, on="industry", how="left").fillna(0)

        df_final[["advancers","decliners","noChanges"]] = df_final[["advancers","decliners","noChanges"]].astype(np.int32)

        cols_order = [
            "industry",
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
        df_final.to_sql(
            name="industry",
            schema="details",
            con=enginedb,
            if_exists="replace",
            index=False,
        )

        logging.info("Đã lưu industry với trend count và weight")

    except Exception:
        logging.exception("Lỗi lưu industry")
        raise
    finally:
        if enginedb is not None:
            enginedb.dispose()
            logging.info("🔌 Đã đóng kết nối DB")
