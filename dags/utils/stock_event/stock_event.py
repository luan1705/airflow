from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import date,timedelta
import numpy as np
import concurrent.futures
from .event import get_event
from .list.HOSE import hose
from .list.HNX import hnx
from .list.UPCOM import upcom
from .list.DEV import derivative
from .list.CW  import warrant

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
log=logging.getLogger(__name__)

enginedb=create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
                        pool_size=10,         # số connection trong pool
                        max_overflow=20,      # số connection ngoài pool nếu cần
                        pool_pre_ping=True )

def stock_event(symbol, max_retries=3, retry_delay=2):
    try:
        data=None
        for attempt in range(1, max_retries + 1):
            try:
                data= get_event(symbol)
                if not data.empty:
                    break  # có dữ liệu thì thoát retry
                else:
                    log.warning(f"⚠️ {symbol}: lần {attempt}/{max_retries} không có dữ liệu.")
            except Exception as e:
                log.error(f"❌ {symbol}: lỗi get_event (lần {attempt}/{max_retries}) - {e}", exc_info=True)
                
            if attempt < max_retries:
                import time
                time.sleep(retry_delay)
        
        if data is None or data.empty:
            msg = f"⚠️ {symbol}: không có dữ liệu sau {max_retries} lần thử, bỏ qua."
            log.info(msg)
            return msg
        
        data.to_sql(
            name=symbol,
            schema="stock_event",
            con=enginedb,
            if_exists="replace",   # hoặc "append"
            index=False,
            method="multi"
        )
        msg = f"✅ {symbol}: lưu thành công."
        log.info(msg)
        return msg
    except Exception as e:
        msg = f"❌ {symbol}: lỗi khi lưu - {e}"
        log.error(msg, exc_info=True)
        return msg

def update_all_stocks(symbol_list):
    """Chạy đa luồng cho 1 danh sách mã"""
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(stock_event, sym): sym for sym in symbol_list}
        for future in concurrent.futures.as_completed(futures):
            try:
                msg = future.result()
                if msg:
                    messages.append(msg)
            except Exception as e:
                # Không để task fail, vẫn log và tiếp tục
                sym = futures[future]
                err_msg = f"❌ {sym}: task bị lỗi không mong muốn - {e}"
                log.error(err_msg, exc_info=True)
                messages.append(err_msg)
    return messages

def save_stock_event():
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    result = []
    result += update_all_stocks(hose)
    result += update_all_stocks(hnx)
    result += update_all_stocks(upcom)
    result += update_all_stocks(derivative)
    result += update_all_stocks(warrant)

    # Tổng hợp kết quả
    errors = [m for m in result if m.startswith("❌")]
    skipped = [m for m in result if m.startswith("⚠️")]
    success = [m for m in result if m.startswith("✅")]

    log.info(f"📊 Tổng số mã xử lý: {len(result)}")
    log.info(f"✅ Thành công: {len(success)} | ⚠️ Bỏ qua: {len(skipped)} | ❌ Lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    enginedb.dispose()
    return result