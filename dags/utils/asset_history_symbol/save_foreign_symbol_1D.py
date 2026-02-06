from sqlalchemy import create_engine,text
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
from .foreign_symbol_1D import foreign_history_1D

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)

enginedb=create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
                        pool_pre_ping=True,       # kiểm tra kết nối trước khi dùng lại
                        pool_size=5,              # số connection giữ trong pool
                        max_overflow=10 
                        )

def get_foreign_symbol_1D(symbol):
    try:
        ketthuc=date.today()
        batdau=ketthuc-timedelta(days=30)
        data=foreign_history_1D(symbol,time='days',start=batdau.strftime('%Y-%m-%d'),end=ketthuc.strftime('%Y-%m-%d'))
        col=['Mã CP','Thời điểm GD','Tổng KLGD ròng', 'Tổng GTGD ròng']
        data=data[col]
        data.columns=['symbol','time','netVol','netVal']
        if data.empty:
            msg = f"⚠️ Không có dữ liệu cho {symbol}, bỏ qua lưu DB."
            logging.info(msg)
            return [msg]
        
        data.columns=['symbol','time','netVol','netVal']
        
        with enginedb.begin() as conn:
            # Kiểm tra xem bảng có tồn tại không
            check_sql = f"SELECT to_regclass('asset_foreign_history.\"{symbol}_1D\"');"
            exists = conn.execute(text(check_sql)).scalar()

            if exists:
                conn.execute(text(f'TRUNCATE TABLE "asset_foreign_history"."{symbol}_1D";'))
            else:
                logging.warning(f"Bảng {symbol}_1D chưa tồn tại, sẽ tạo mới.")
    
            data.to_sql(name=f'{symbol}_1D',
                        schema='asset_foreign_history',
                        con=conn,
                        if_exists='append',
                        index=False
                        )
        msg = f"✅ Đã lưu {symbol}"
        logging.info(msg)
        return [msg]
    except Exception as e:
        msg = f"❌ Lỗi lưu {symbol}: {e}"
        logging.exception(msg)
        return [msg]

def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_foreign_symbol_1D, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result() or []  # luôn là list
            messages.extend(result)
    return messages

def save_all_foreign_1D():
    logging.info("🚀 Bắt đầu cập nhật dữ liệu...")
    result = []
    result += update_all_stocks(hose)
    result += update_all_stocks(hnx)
    result += update_all_stocks(upcom)
    result += update_all_stocks(derivative)
    result += update_all_stocks(warrant)

    errors = [msg for msg in result if msg.startswith("❌")]
    warnings = [msg for msg in result if msg.startswith("⚠️")]

    logging.info(f"📊 Tổng số mã xử lý: {len(result)}")
    logging.info(f"❌ Tổng số lỗi: {len(errors)}")
    logging.info(f"⚠️ Không có dữ liệu: {len(warnings)}")

    if errors:
        logging.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            logging.warning(err)

    logging.info("🎉 Hoàn thành cập nhật tất cả mã.")
    enginedb.dispose()
    return result