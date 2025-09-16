from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import date,timedelta
import numpy as np
import concurrent.futures
from .list.HOSE import hose
from .list.HNX import hnx
from .list.UPCOM import upcom
from .list.DEV import derivative
from .list.CW  import warrant
from .foreign_history import foreign_history

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
log=logging.getLogger(__name__)

def save_foreign_history(symbol):
    try:
        data=foreign_history(symbol,time='days',start='2000-01-01',end='2030-01-01')
        col=['Mã CP','Thời điểm GD','Tổng KLGD ròng', 'Tổng GTGD ròng']
        data=data[col]
        data.columns=['symbol','time','netVol','netVal']
        if data.empty:
            msg = f"⚠️ Không có dữ liệu cho {symbol}, bỏ qua lưu DB."
            log.info(msg)
            return [msg]
        
        data.columns=['symbol','time','netVol','netVal']
    
        enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
    
        data.to_sql(name=f'{symbol}',
                    schema='foreign_history',
                    con=enginedb,
                    if_exists='replace',
                    index=False
                    )
        msg = f"✅ Đã lưu {symbol}"
        log.info(msg)
        return [msg]
    except Exception as e:
        msg = f"❌ Lỗi lưu {symbol}: {e}"
        log.exception(msg)
        return [msg]

def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(save_foreign_history, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result() or []  # luôn là list
            messages.extend(result)
    return messages

def save_all_foreign():
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    result = []
    result += update_all_stocks(hose)
    result += update_all_stocks(hnx)
    result += update_all_stocks(upcom)
    result += update_all_stocks(derivative)
    result += update_all_stocks(warrant)

    errors = [msg for msg in result if msg.startswith("❌")]
    warnings = [msg for msg in result if msg.startswith("⚠️")]

    log.info(f"📊 Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")
    log.info(f"⚠️ Không có dữ liệu: {len(warnings)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return result