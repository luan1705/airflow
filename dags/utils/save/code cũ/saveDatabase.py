from sqlalchemy import create_engine
import concurrent.futures
from datetime import datetime
from VNSfinance import history
from .List.HOSE import hose
from .List.HNX import hnx
from .List.UPCOM import upcom
import logging

# Thiết lập logging
log = logging.getLogger(__name__)

# Kết nối PostgreSQL
engine = create_engine(
    'postgresql://testtest:testtest@videv.cloud:5432/testtest',
    pool_size=20,
    max_overflow=30,
    pool_timeout=60
)

# Tính các chỉ báo kỹ thuật
def indicators(data):
    data['MA10'] = data['close'].rolling(window=10).mean()
    data['MA20'] = data['close'].rolling(window=20).mean()
    data['MA50'] = data['close'].rolling(window=50).mean()
    data['EMA_12'] = data['close'].ewm(span=12, adjust=False).mean()
    data['EMA_26'] = data['close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = data['EMA_12'] - data['EMA_26']
    data['Signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()
    data['volume_20'] = data['volume'].rolling(window=20).mean()
    return data

# Hàm cập nhật dữ liệu cho một mã cổ phiếu
def get_stock(symbol, schema):
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        stock = history(symbol=symbol, start='2008-01-01', end=today, interval='ONE_DAY')

        if stock is None or stock.empty:
            msg = f"⚠️ Không có dữ liệu cho {schema}.{symbol}"
            log.warning(msg)
            return msg

        stock = stock.round(2)
        stock = indicators(stock)

        stock.to_sql(
            name=symbol,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False
        )

        msg = f"✅ Đã lưu {schema}.{symbol}"
        log.info(msg)
        return msg

    except Exception as e:
        msg = f"❌ Lỗi khi xử lý {schema}.{symbol}: {str(e)}"
        log.error(msg)
        return msg

# Cập nhật toàn bộ danh sách mã cho một sàn
def update_all_stocks(symbol_list, schema):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_stock, symbol, schema) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.append(future.result())
    return messages

# Hàm tổng cho tất cả sàn — để DAG gọi
def save_DB():
    log.info("🚀 Bắt đầu cập nhật dữ liệu cho tất cả sàn...")
    result = []
    result += update_all_stocks(hose, 'HOSE')
    result += update_all_stocks(hnx, 'HNX')
    result += update_all_stocks(upcom, 'UPCOM')

    errors = [msg for msg in result if msg.startswith("❌") or msg.startswith("⚠️")]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if len(errors) >= 5:
        raise Exception("Task thất bại vì có lỗi:\n" + "\n".join(errors))

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return result
