from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import date,timedelta

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)

def proprietary_HNX():
    try:
        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        logging.info('Kết nối DB')
        df = pd.read_sql('SELECT * FROM "market_history"."proprietary_HNX_1D"', con=enginedb)
        df = df[df['tradingDate'].eq(df['tradingDate'].max())].copy()
        ngay=df['tradingDate'].iloc[0]
        net_match_val= int(df['netMatchVal'].sum())
        net_deal_val= int(df['netDealVal'].sum())
        net_val= int(net_match_val + net_deal_val)
        net_match_vol= int(df['netMatchVol'].sum())
        net_deal_vol= int(df['netDealVol'].sum())
        net_vol= int(net_match_vol + net_deal_vol)
        propietary_ALL=pd.DataFrame([['date',ngay],
                                    ['net_match_val',net_match_val],
                                    ['net_deal_val',net_deal_val],
                                    ['net_val',net_val],
                                    ['net_match_vol',net_match_vol],
                                    ['net_deal_vol',net_deal_vol],
                                    ['net_vol',net_vol],
                                    ],columns=['key','value'])

        propietary_ALL.to_sql(name='proprietary_HNX',
                             schema='market_data',
                             con=enginedb,
                             if_exists='replace',
                             index=False
                            )
        logging.info('Đã lưu propietary_HNX')
    except Exception as E:
        logging.exception('Lỗi lưu propietary_HNX')
if __name__=='__main__':
    proprietary_HNX()