from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import date, timedelta
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

def industry():
    enginedb = None
    try:
        enginedb = create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")
        logging.info('Kết nối DB')

        # === Lấy dữ liệu gốc ===
        df = pd.read_sql("""
                         SELECT *
                         FROM "details"."asset" left join "info"."asset" using("symbol")
                        """, con=enginedb)

        # === Tính xu hướng (up/down/side) theo open/refPrice ===
        df = df.dropna(subset=['open', 'refPrice', 'industry'])  # loại bỏ dòng lỗi
        df['upTrend_flag'] = np.where(df['close'] > df['refPrice'], 1, 0)
        df['downTrend_flag'] = np.where(df['close'] < df['refPrice'], 1, 0)
        df['sideWay_flag'] = np.where(df['close'] == df['refPrice'], 1, 0)

        # === Tính số lượng xu hướng theo ngành ===
        trend_count = (
            df.groupby('industry')[['upTrend_flag', 'downTrend_flag', 'sideWay_flag']]
              .sum()
              .reset_index()
              .rename(columns={
                  'upTrend_flag': 'advancers',
                  'downTrend_flag': 'decliners',
                  'sideWay_flag': 'noChanges'
              })
        )

        # === Tính các chỉ số ngành như cũ ===
        df['foreignNetVal']= df['foreignBuyVal'] - df['foreignSellVal']
        cols = ['industry', 'totalVal', 'foreignNetVal', 'marketCap', 'pe', 'pb', 'matchRatioChange']
        df = df[cols]
        df['profit'] = df['marketCap'] / df['pe']
        df['equity'] = df['marketCap'] / df['pb']
        df['totalMarketCap_industry'] = df.groupby('industry')['marketCap'].transform('sum')
        df['marketCapweigh'] = df['marketCap'] / df['totalMarketCap_industry']
        df['matchRatioChange'] = df['matchRatioChange'] * df['marketCapweigh']

        df = df.fillna(0)
        df_clean = df[~df.isin([np.inf, -np.inf]).any(axis=1)]
        df_clean = df_clean.groupby('industry').sum(numeric_only=True).reset_index()
        df_clean = df_clean[df_clean['industry'] != 0]
        df_clean = df_clean.drop(columns=['pe', 'pb', 'totalMarketCap_industry', 'marketCapweigh'])
        df_clean['pe'] = df_clean['marketCap'] / df_clean['profit']
        df_clean['pb'] = df_clean['marketCap'] / df_clean['equity']

        colsindustry = ['industry', 'totalVal', 'foreignNetVal', 'marketCap', 'pe', 'pb', 'matchRatioChange']
        df_clean = df_clean[colsindustry]

        # === Thêm cột weight (tỉ trọng ngành) ===
        total_marketcap = df_clean['marketCap'].sum()
        df_clean['weight'] = (df_clean['marketCap'] / total_marketcap) * 100

        # === Gộp xu hướng vào bảng ngành ===
        df_final = pd.merge(df_clean, trend_count, on='industry', how='left').fillna(0)

        # 🔧 Ép kiểu các cột xu hướng về int32
        df_final['advancers'] = df_final['advancers'].astype(np.int32)
        df_final['decliners'] = df_final['decliners'].astype(np.int32)
        df_final['noChanges'] = df_final['noChanges'].astype(np.int32)

        # === Ghi kết quả ra DB ===
        df_final.to_sql(
            name='industry',
            schema='details',
            con=enginedb,
            if_exists='replace',
            index=False
        )

        logging.info('Đã lưu industry với trend count và weight')

    except Exception as E:
        logging.exception('Lỗi lưu industry')
    finally:
        if enginedb is not None:
            enginedb.dispose()
            logging.info("🔌 Đã đóng kết nối DB")
