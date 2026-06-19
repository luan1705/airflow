from sqlalchemy import create_engine
from psycopg2.extras import execute_values
import pandas as pd
import logging
from datetime import datetime, timedelta
from .foreign_history_1D import foreign_history

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
SCHEMA = "exchange_history"
symbols=['HOSE','HNX','UPCOM']
def foreign(symbol,enginedb):
    try:
        logging.info('Kết nối DB')
        start=(datetime.today()-timedelta(days=15)).strftime("%Y-%m-%d")
        df=foreign_history(f"{symbol}",start)

        table_name = f"foreign_{symbol}_1D"
        cols_quoted = ",".join(f'"{c}"' for c in df.columns)
        rows = [tuple(x) for x in df.to_numpy()]
        
        insert_sql = f"""
            INSERT INTO "{SCHEMA}"."{table_name}" ({cols_quoted})
            VALUES %s
            ON CONFLICT ("time") DO UPDATE SET
            "buyVol"       = EXCLUDED."buyVol",
            "buyVal"       = EXCLUDED."buyVal",
            "sellVol"      = EXCLUDED."sellVol",
            "sellVal"      = EXCLUDED."sellVal",
            "netVol"       = EXCLUDED."netVol",
            "netVal"       = EXCLUDED."netVal";
        """

        with enginedb.begin() as conn:
            cur = conn.connection.cursor()
            try:
                execute_values(cur, insert_sql, rows, page_size=1000)
            finally:
                cur.close()
                
        logging.info(f'Đã lưu foreign_{symbol}')
    except Exception as E:
        logging.exception(f'Lỗi lưu foreign_{symbol}')
        
def main():
    enginedb = create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech")
    try:
        logging.info("Kết nối DB thành công")
        for sym in symbols:
            foreign(sym,enginedb)
        
    finally:
        enginedb.dispose()
        logging.info("🔒 Đã đóng kết nối DB")