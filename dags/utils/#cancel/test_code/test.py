from sqlalchemy import create_engine,text
import pandas as pd
from datetime import datetime, timedelta
engine = create_engine(
"postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
)
today = datetime.now().date()
batdau=today-timedelta(days=50)
symbol = "ACB"
def test():

    query = f"""
        SELECT *
        FROM ohlcv."{symbol}_1D"
        WHERE time >= '{batdau} and '
        """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        log.warning(f"⚠ Không có bảng ohlcv.{symbol}_1D")
        return pd.DataFrame()