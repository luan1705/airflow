from sqlalchemy import create_engine, text
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

def foreign_HOSE():
    try:    
        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        logging.info('Kết nối DB')
        df = pd.read_sql('SELECT * FROM "market_history"."foreign_HOSE_1"', con=enginedb)
        buy_Vol = df['buyVol'].sum()
        sell_Vol  = df['sellVol'].sum()
        net_Vol = df['netVol'].sum()
        buy_Val = df['buyVal'].sum()
        sell_Val  = df['sellVal'].sum()
        net_Val = df['netVal'].sum()
        foreign_HOSE_df=pd.DataFrame([['buy_Vol',buy_Vol],
                            ['sell_Vol',sell_Vol],
                            ['net_Vol',net_Vol],
                            ['buy_Val',buy_Val],
                            ['sell_Val',sell_Val],
                            ['net_Val',net_Val],
                            ],columns=['key','value'])
        foreign_HOSE_df.to_sql(name='foreign_HOSE',
                             schema='market_data',
                             con=enginedb,
                             if_exists='replace',
                             index=False
                            )
        logging.info('Đã lưu foreign_HOSE')
        
        df['time'] = pd.to_datetime(df['time'])
        ngay_moi_nhat = df['time'].max().date()

        row = {
            "time": ngay_moi_nhat,
            "buyVol": int(buy_Vol),
            "sellVol": int(sell_Vol),
            "netVol": int(net_Vol),
            "buyVal": int(buy_Val),
            "sellVal": int(sell_Val),
            "netVal": int(net_Val)
        }

        # UPSERT vào bảng foreign_HOSE_1D
        with enginedb.begin() as conn:
            conn.execute(text("""
                INSERT INTO "market_history"."foreign_HOSE_1D"
                ("time", "buyVol", "sellVol", "netVol", "buyVal", "sellVal", "netVal")
                VALUES (:time, :buyVol, :sellVol, :netVol, :buyVal, :sellVal, :netVal)
                ON CONFLICT ("time") DO UPDATE SET
                    "buyVol" = EXCLUDED."buyVol",
                    "sellVol" = EXCLUDED."sellVol",
                    "netVol" = EXCLUDED."netVol",
                    "buyVal" = EXCLUDED."buyVal",
                    "sellVal" = EXCLUDED."sellVal",
                    "netVal" = EXCLUDED."netVal";
            """), row)

        logging.info(f'Đã upsert dữ liệu ngày {ngay_moi_nhat} vào foreign_HOSE_1D')
        
    except Exception as E:
        logging.exception('Lỗi lưu foreign_HOSE')
if __name__=='__main__':
    foreign_HOSE()