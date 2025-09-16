from sqlalchemy import create_engine,text
import pandas as pd
import logging
from .foreign_history_1D import foreign_history

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
symbols=['ALL','HOSE','HNX','UPCOM']
def foreign(symbol):
    try:    
        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        logging.info('Kết nối DB')

        foreign=foreign_history(f"{symbol}","2025-01-01")
        
        with enginedb.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "market_history"."foreign_{symbol}_1D";'))
        
        foreign.to_sql(name=f'foreign_{symbol}_1D',
                             schema='market_history',
                             con=enginedb,
                             if_exists='append',
                             index=False
                            )
        logging.info(f'Đã lưu foreign_{symbol}')
    except Exception as E:
        logging.exception(f'Lỗi lưu foreign_{symbol}')
def main():
    for sym in symbols:
        foreign(sym)
        
if __name__=='__main__':
    main()