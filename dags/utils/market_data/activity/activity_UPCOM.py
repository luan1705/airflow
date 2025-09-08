from sqlalchemy import create_engine
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)

def activity_UPCOM():
    try:    
        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        logging.info('Kết nối DB')
        df = pd.read_sql('SELECT * FROM "history_data"."eboard_table"', con=enginedb).sort_values(by='symbol',ascending=True)

        upcom = df[df['exchange'] == 'UPCOM']

        advancers     = upcom[upcom['matchChange'] > 0].shape[0]
        noChange      = upcom[upcom['matchChange'] == 0].shape[0]
        decliners     = upcom[upcom['matchChange'] < 0].shape[0]

        advancers_Val = upcom[upcom['matchChange'] > 0]['totalVal'].sum().astype(int)
        noChange_Val  = upcom[upcom['matchChange'] == 0]['totalVal'].sum().astype(int)
        decliners_Val = upcom[upcom['matchChange'] < 0]['totalVal'].sum().astype(int)

        totalVol      = upcom['totalVol'].sum().astype(int)
        totalVal      = upcom['totalVal'].sum().astype(int)

        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        dfindex = pd.read_sql('SELECT * FROM "history_data"."indices"', con=enginedb).sort_values(by='symbol',ascending=True)
        point=float(dfindex.loc[dfindex['symbol']=='UpcomIndex','point'].values[0])

        activity_UPCOM=pd.DataFrame([['advancers',advancers],
                                    ['noChange',noChange],
                                    ['decliners',decliners],
                                    ['advancers_Val',advancers_Val],
                                    ['noChange_Val',noChange_Val],
                                    ['decliners_Val',decliners_Val],
                                    ['totalVol',totalVol],
                                    ['totalVal',totalVal],
                                    ['UPCOMINDEX',point],
                                    ],columns=['key','value'])
        activity_UPCOM.to_sql(name='activity_UPCOM',
                             schema='market_data',
                             con=enginedb,
                             if_exists='replace',
                             index=False
                            )
        logging.info('Đã lưu activity_UPCOM')
    except Exception as E:
        logging.exception('Lỗi lưu activity_UPCOM')
if __name__=='__main__':
    activity_UPCOM()