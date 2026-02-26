from sqlalchemy import create_engine,text
import pandas as pd
import logging
from datetime import date,timedelta
import numpy as np
import concurrent.futures
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW
from .proprietary_history import proprietary_history

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
log=logging.getLogger(__name__)

enginedb = create_engine(
    'postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech',
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

def get_proprietary_history(symbol):
    try:
        df = pd.read_sql('SELECT * FROM "market_history"."proprietary_HOSE_1D"', con=enginedb)
        ngay_moi_nhat=df['time'].max()
        data=proprietary_history(symbol,time='days',start=ngay_moi_nhat,end=ngay_moi_nhat)
        if data.empty:
            msg = f"⚠️ Không có dữ liệu cho {symbol}, bỏ qua lưu DB."
            log.info(msg)
            return [msg]
        col=['Mã CP','Thời điểm GD','Tổng KLGD ròng','Tổng GTGD ròng']
        data=data[col]
        data.columns=['symbol','time','netVol','netVal']
        data["time"] = pd.to_datetime(data["time"])
        latest_row = data.loc[data["time"].idxmax()]
        ngay_moi_nhat = latest_row['time'].date()
        row = {
            "symbol": symbol,
            "time": ngay_moi_nhat,
            "netVol": int(latest_row['netVol']),
            "netVal": int(latest_row['netVal'])
        }
        with enginedb.begin() as cur:
            cur.execute(text("""
                INSERT INTO proprietary_history.proprietary_history
                ("symbol", "time", "netVol", "netVal")
                VALUES (:symbol, :time, :netVol, :netVal)
                ON CONFLICT ("symbol") DO UPDATE SET
                    "time"   = EXCLUDED."time",
                    "netVol" = EXCLUDED."netVol",
                    "netVal" = EXCLUDED."netVal";
            """), row)

        msg = f"✅ Upsert {symbol} ngày {ngay_moi_nhat} OK"
        log.info(msg)
        return [msg]
    except Exception as e:
        msg = f"❌ Lỗi lưu {symbol}: {e}"
        log.exception(msg)
        return [msg]

def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(get_proprietary_history, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result() or []  # luôn là list
            messages.extend(result)
    return messages

def save_proprietary_history():
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    result = []
    result += update_all_stocks(HOSE)
    result += update_all_stocks(HNX)
    result += update_all_stocks(UPCOM)
    result += update_all_stocks(DERIVATIVES)
    result += update_all_stocks(CW)

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
    enginedb.dispose()
    return result