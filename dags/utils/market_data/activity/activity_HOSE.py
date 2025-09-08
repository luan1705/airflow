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

def activity_HOSE():
    try:    
        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        logging.info('Kết nối DB')
        df = pd.read_sql('SELECT * FROM "history_data"."eboard_table"', con=enginedb).sort_values(by='symbol',ascending=True)

        hose = df[df['exchange'] == 'HOSE']

        advancers     = hose[hose['matchChange'] > 0].shape[0]
        noChange      = hose[hose['matchChange'] == 0].shape[0]
        decliners     = hose[hose['matchChange'] < 0].shape[0]

        advancers_Val = hose[hose['matchChange'] > 0]['totalVal'].sum().astype(int)
        noChange_Val  = hose[hose['matchChange'] == 0]['totalVal'].sum().astype(int)
        decliners_Val = hose[hose['matchChange'] < 0]['totalVal'].sum().astype(int)

        totalVol      = hose['totalVol'].sum().astype(int)
        totalVal      = hose['totalVal'].sum().astype(int)

        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        dfindex = pd.read_sql('SELECT * FROM "history_data"."indices"', con=enginedb).sort_values(by='symbol',ascending=True)
        point=float(dfindex.loc[dfindex['symbol']=='VNINDEX','point'].values[0])

        activity_HOSE=pd.DataFrame([['advancers',advancers],
                                    ['noChange',noChange],
                                    ['decliners',decliners],
                                    ['advancers_Val',advancers_Val],
                                    ['noChange_Val',noChange_Val],
                                    ['decliners_Val',decliners_Val],
                                    ['totalVol',totalVol],
                                    ['totalVal',totalVal],
                                    ['VNINDEX',point],
                                    ],columns=['key','value'])
        activity_HOSE.to_sql(name='activity_HOSE',
                             schema='market_data',
                             con=enginedb,
                             if_exists='replace',
                             index=False
                            )
        logging.info('Đã lưu activity_HOSE')
    except Exception as E:
        logging.exception('Lỗi lưu activity_HOSE')
if __name__=='__main__':
    activity_HOSE()