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

enginedb=create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")

def tradingview(symbol):
    try:    
        logging.info('Kết nối DB')
        df = pd.read_sql(f'SELECT "symbol","close" FROM "ohlcv"."{symbol}_1D" ORDER BY time DESC LIMIT 1', con=enginedb)
        logging.info(f'Đã lấy dữ liệu cho mã {symbol}')
        return df
    except Exception as E:
        logging.exception('Lỗi lấy dữ liệu từ DB')
    finally:
        enginedb.dispose()
        logging.info("🔒 Đã đóng kết nối DB")

# if __name__ == "__main__":
#     symbol = 'VN30'
#     tradingview(symbol)