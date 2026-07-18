from sqlalchemy import create_engine,text
import pandas as pd
import logging
from datetime import date,timedelta
import numpy as np
import concurrent.futures
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW
from .def_asset_foreign_history import foreign_history_1D

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)

enginedb=create_engine("postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
                        pool_pre_ping=True,       # kiểm tra kết nối trước khi dùng lại
                        pool_size=5,              # số connection giữ trong pool
                        max_overflow=10 
                        )
def get_foreign_symbol_1M(symbol):
    try:
        ketthuc=date.today()
        batdau=ketthuc-timedelta(days=365)
        data=foreign_history_1D(symbol,time='months',start=batdau.strftime('%Y-%m'),end=ketthuc.strftime('%Y-%m'))
        col=['Mã CP','Thời điểm GD','Tổng KLGD ròng', 'Tổng GTGD ròng']
        data=data[col]
        data.columns=['symbol','time','netVol','netVal']
        if data.empty:
            with enginedb.begin() as conn:
                check_sql = f"SELECT to_regclass('asset_foreign_history.\"{symbol}_1M\"');"
                exists = conn.execute(text(check_sql)).scalar()
                if not exists:
                    data.to_sql(
                        name=f'{symbol}_1M',
                        schema='asset_foreign_history',
                        con=conn,
                        if_exists='append',
                        index=False
                    )
            msg = f"⚠️ Không có dữ liệu cho {symbol}, đã tạo bảng rỗng nếu chưa tồn tại."
            logging.info(msg)
            return [msg]

        data.columns=['symbol','time','netVol','netVal']

        with enginedb.begin() as conn:
            # Kiểm tra xem bảng có tồn tại không
            check_sql = f"SELECT to_regclass('asset_foreign_history.\"{symbol}_1M\"');"
            exists = conn.execute(text(check_sql)).scalar()

            if exists:
                conn.execute(text(f'TRUNCATE TABLE "asset_foreign_history"."{symbol}_1M";'))
            else:
                logging.warning(f"Bảng {symbol}_1M chưa tồn tại, sẽ tạo mới.")

            data.to_sql(name=f'{symbol}_1M',
                        schema='asset_foreign_history',
                        con=conn,
                        if_exists='append',
                        index=False
                        )
        msg = f"✅ Đã lưu {symbol}_1M"
        logging.info(msg)
        return [msg]
    except Exception as e:
        msg = f"❌ Lỗi lưu {symbol}: {e}"
        logging.exception(msg)
        return [msg]

def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_foreign_symbol_1M, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result() or []  # luôn là list
            messages.extend(result)
    return messages

def save_all_foreign_1M():
    logging.info("🚀 Bắt đầu cập nhật dữ liệu...")
    result = []
    result += update_all_stocks(HOSE)
    result += update_all_stocks(HNX)
    result += update_all_stocks(UPCOM)
    result += update_all_stocks(DERIVATIVES)
    result += update_all_stocks(CW)

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
