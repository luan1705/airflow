from sqlalchemy import create_engine
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def impact():
    enginedb = create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech")

    try:
        logging.info("Kết nối DB")

        # ====== LOAD asset ======
        df1 = pd.read_sql(
            """
            SELECT * 
            FROM "details"."asset" left join "info"."asset" using("symbol")
            WHERE "matchRatioChange" != -100
            """,
            con=enginedb
        )
        data = df1[df1["exchange"].isin(["HOSE", "HNX", "UPCOM"])].copy()
        data = data[["symbol", "exchange", "matchRatioChange", "marketCap"]].fillna(0)

        # ====== LOAD INDEX ======
        df2 = pd.read_sql('SELECT * FROM "indices"."vietnam"', con=enginedb)
        df2 = df2[df2["symbol"].isin(["HNXINDEX", "UPCOMINDEX", "VNINDEX"])].copy()

        mapping = {"HNXINDEX": "HNX", "UPCOMINDEX": "UPCOM", "VNINDEX": "HOSE"}
        df2["point"] = df2["point"].astype("Float64")
        df2["exchange"] = df2["symbol"].replace(mapping)
        df2 = df2[["exchange", "point"]]

        # ====== MERGE + CALC impact theo từng sàn ======
        data = data.merge(df2, on="exchange", how="left")

        data["exchangetotalmarketcap"] = data.groupby("exchange")["marketCap"].transform("sum")
        data["impact"] = round(
            (data["marketCap"] / data["exchangetotalmarketcap"]) * (data["matchRatioChange"] / 100) * data["point"],
            2
        )

        # ====== ✅ CHỈ LƯU 1 BẢNG GỘP ======
        out = data[["exchange", "symbol", "impact"]].copy()
        out = out.sort_values(by=["exchange", "impact"], ascending=[True, False]).reset_index(drop=True)

        out.to_sql(
            name="exchange_impact",
            schema="details",
            con=enginedb,
            if_exists="replace",
            index=False
        )

        logging.info("✅ Đã lưu market_data.impact_exchange")

    except Exception:
        logging.exception("❌ Lỗi lưu impact_exchange")
        raise
    finally:
        enginedb.dispose()
        logging.info("🔒 Đã đóng kết nối DB")
