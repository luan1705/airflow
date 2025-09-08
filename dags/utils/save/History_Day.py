from sqlalchemy import create_engine
import concurrent.futures
from datetime import datetime
from .tradingview import tradingview
from .List.HOSE import hose
from .List.HNX import hnx
from .List.UPCOM import upcom
from .List.DEV import derivative
from .List.CW  import warrant
import time
import logging 

# Thiết lập logging 
log=logging.getLogger(__name__)

# Kết nối PostgreSQL
engine = create_engine(# method://user:pass@host:port/dbName
                       'postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech',
                        pool_size=10,
                        max_overflow=20,
                        pool_timeout=60
                        )

# Hàm cập nhật dữ liệu cho một mã cổ phiếu
def get_stock(symbol):
    try:
        time.sleep(2)
        today = datetime.now().strftime('%Y-%m-%d')

        # Lấy dữ liệu lịch sử
        stock = tradingview(symbol=symbol, start='2006-01-01', end=today, time='days')
        stock['Exchange'] = 'HOSE' if symbol in hose else 'HNX' if symbol in hnx else 'UPCOM'

        # Kiểm tra dữ liệu trả về
        if stock is None or stock.empty:
            msg = f"⚠️ Không có dữ liệu cho {symbol}"
            log.warning(msg)
            return msg

        # Ghi vào PostgreSQL
        stock.to_sql(
            name=f'{symbol}_1D',
            con=engine,
            schema = 'history_tradingview',
            if_exists='replace',
            index=False,
            chunksize= 800 
        )
    
        msg = f"✅ Đã lưu {symbol}"
        log.info(msg)
        return msg

    except Exception as e:
        msg = f"❌ Lỗi khi xử lý {symbol}: {str(e)}"
        log.error(msg)
        return msg

# Cập nhật toàn bộ danh sách mã cho một sàn
def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(get_stock, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.append(future.result())
    return messages


# Hàm tổng cho tất cả sàn — để DAG gọi
def save_DB_1D():
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    result = []
    result += update_all_stocks(hose)
    result += update_all_stocks(hnx)
    result += update_all_stocks(upcom)

    errors = [msg for msg in result if msg.startswith("❌") or msg.startswith("⚠️")]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    # if len(errors) >= 5:
    #     raise Exception("Task thất bại vì có lỗi:\n" + "\n".join(errors))

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return result
