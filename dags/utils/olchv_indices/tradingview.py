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

enginedb = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    pool_size=10, max_overflow=20, pool_timeout=60,
    pool_pre_ping=True, pool_recycle=1800,
)

def tradingview(symbol):
    try:    
        logging.info('Kết nối DB')
        df = pd.read_sql(f'SELECT "symbol","close","open","high","low" FROM "ohlcv"."{symbol}_1D" ORDER BY time DESC LIMIT 1', con=enginedb)
        logging.info(f'Đã lấy dữ liệu cho mã {symbol}')
        return df
    except Exception as E:
        logging.exception('Lỗi lấy dữ liệu từ DB')
        return pd.DataFrame()

# if __name__ == "__main__":
#     symbol = 'HNX30'
#     tradingview(symbol)