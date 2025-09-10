from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import date,timedelta
import numpy as np

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
def industry():
    try:
        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        logging.info('Kết nối DB')
        df = pd.read_sql('SELECT * FROM "history_data"."stock_details"', con=enginedb)

        cols=['industry','totalVal','foreignNetValue','marketCap','pe','pb','ratioChange']
        df=df[cols]
        df['profit']=df['marketCap']/df['pe']
        df['equity']=df['marketCap']/df['pb']
        df['totalMarketCap_industry'] = df.groupby('industry')['marketCap'].transform('sum')
        df['marketCapweigh']=df['marketCap']/df['totalMarketCap_industry']
        df['ratioChangeIndustry']=df['ratioChange']*df['marketCapweigh']
        df=df.fillna(0)
        df_clean = df[~df.isin([np.inf, -np.inf]).any(axis=1)]
        df_clean=df_clean.groupby('industry').sum(numeric_only=True).reset_index()
        df_clean=df_clean[df_clean['industry']!=0]
        df_clean=df_clean.drop(columns=['pe','pb','totalMarketCap_industry','marketCapweigh'])
        df_clean['pe']=df_clean['marketCap']/df_clean['profit']
        df_clean['pb']=df_clean['marketCap']/df_clean['equity']
        colsindustry=['industry','totalVal','foreignNetValue','marketCap','pe','pb','ratioChangeIndustry']
        df_clean=df_clean[colsindustry]

        df_clean.to_sql(name='industry_details',
                             schema='history_data',
                             con=enginedb,
                             if_exists='replace',
                             index=False
                            )
        logging.info('Đã lưu industry')
    except Exception as E:
        logging.exception('Lỗi lưu industry')
