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
def impact():
    try:
        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        logging.info('Kết nối DB')
        df1 = pd.read_sql('SELECT * FROM "history_data"."stock_details"', con=enginedb)
        all1 = df1[df1['exchange'].isin(['HOSE','HNX','UPCOM'])]
        cols=['symbol','exchange','ratioChange','marketCap']
        all1=all1[cols].fillna(0)
        df2 = pd.read_sql('SELECT * FROM "history_data"."indices"', con=enginedb)
        df2=df2[df2['symbol'].isin(['HNXIndex','UpcomIndex','VNINDEX'])]
        mapping = {
            "HNXIndex": "HNX",
            "UpcomIndex": "UPCOM",
            "VNINDEX": "HOSE"
        }
        df2['point']=df2['point'].astype('Float64')
        df2['exchange']=df2['symbol'].replace(mapping)
        colsindice=['exchange','point']
        df2=df2[colsindice]
        data=all1.merge(df2,on='exchange',how='left')
        data['totalmarketCap']=data['marketCap'].sum()
        data['exchangetotalmarketcap']=data.groupby('exchange')['marketCap'].transform('sum')
        data['impactAll']=round((data['marketCap']/data['totalmarketCap'])*(data['ratioChange']/100)*data['point'],2)
        data['impactexchange']=round((data['marketCap']/data['exchangetotalmarketcap'])*(data['ratioChange']/100)*data['point'],2)
        # bảng all
        colsall=['symbol','impactAll']
        dataall=data[colsall].rename(columns={'impactAll':'impact'})
        dataallup=dataall.sort_values(by='impact',ascending=False).reset_index(drop=True)
        dataallup.to_sql(name='impact_ALL_up',
                       schema='market_data',
                       con=enginedb,
                       if_exists='replace',
                       index=False
                       )
        logging.info('Đã lưu impact_ALL_up')
        dataalldown=dataall.sort_values(by='impact',ascending=True).reset_index(drop=True)
        dataalldown.to_sql(name='impact_ALL_down',
                       schema='market_data',
                       con=enginedb,
                       if_exists='replace',
                       index=False
                       )
        logging.info('Đã lưu impact_ALL_down')

        #bảng hose
        datahose=data[data['exchange']=='HOSE']
        colsexchange=['symbol','impactexchange']
        datahose=datahose[colsexchange].rename(columns={'impactexchange':'impact'})
        datahoseup=datahose.sort_values(by='impact',ascending=False).reset_index(drop=True)
        datahoseup.to_sql(name='impact_HOSE_up',
                       schema='market_data',
                       con=enginedb,
                       if_exists='replace',
                       index=False
                       )
        logging.info('Đã lưu impact_HOSE_up')
        datahosedown=datahose.sort_values(by='impact',ascending=True).reset_index(drop=True)
        datahosedown.to_sql(name='impact_HOSE_down',
                       schema='market_data',
                       con=enginedb,
                       if_exists='replace',
                       index=False
                       )
        logging.info('Đã lưu impact_HOSE_down')

        #bảng hnx
        datahnx=data[data['exchange']=='HNX']
        colsexchange=['symbol','impactexchange']
        datahnx=datahnx[colsexchange].rename(columns={'impactexchange':'impact'})
        datahnxup=datahnx.sort_values(by='impact',ascending=False).reset_index(drop=True)
        datahnxup.to_sql(name='impact_HNX_up',
                       schema='market_data',
                       con=enginedb,
                       if_exists='replace',
                       index=False
                       )
        logging.info('Đã lưu impact_HNX_up')
        datahnxdown=datahnx.sort_values(by='impact',ascending=True).reset_index(drop=True)
        datahnxdown.to_sql(name='impact_HNX_down',
                       schema='market_data',
                       con=enginedb,
                       if_exists='replace',
                       index=False
                       )
        logging.info('Đã lưu impact_HNX_down')

        #bảng upcom
        dataupcom=data[data['exchange']=='UPCOM']
        colsexchange=['symbol','impactexchange']
        dataupcom=dataupcom[colsexchange].rename(columns={'impactexchange':'impact'})
        dataupcomup=dataupcom.sort_values(by='impact',ascending=False).reset_index(drop=True)
        dataupcomup.to_sql(name='impact_UPCOM_up',
                       schema='market_data',
                       con=enginedb,
                       if_exists='replace',
                       index=False
                       )
        logging.info('Đã lưu impact_UPCOM_up')
        dataupcomdown=dataupcom.sort_values(by='impact',ascending=True).reset_index(drop=True)
        dataupcomdown.to_sql(name='impact_UPCOM_down',
                       schema='market_data',
                       con=enginedb,
                       if_exists='replace',
                       index=False
                       )
        logging.info('Đã lưu impact_UPCOM_down')
        
        logging.info('Đã lưu hết impact')
    except Exception as E:
        logging.exception('Lỗi lưu impact')
