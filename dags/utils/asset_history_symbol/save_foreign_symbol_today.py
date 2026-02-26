from sqlalchemy import create_engine,text
import pandas as pd
import logging
from datetime import date,timedelta
import numpy as np
import concurrent.futures
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW

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

def ensure_table(symbol):
    """Tạo bảng asset_foreign_history.{symbol}_1D nếu chưa tồn tại, và thêm PK trên cột time"""
    try:
        with enginedb.begin() as cur:
            cur.execute(text(f"""
                CREATE TABLE IF NOT EXISTS asset_foreign_history."{symbol}_1D" (
                    "symbol" TEXT NOT NULL,
                    "time" DATE NOT NULL,
                    "netVol" DOUBLE PRECISION,
                    "netVal" DOUBLE PRECISION
                );
            """))

            cur.execute(text(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'asset_foreign_history."{symbol}_1D"'::regclass
                          AND contype = 'p'
                    ) THEN
                        ALTER TABLE asset_foreign_history."{symbol}_1D"
                        ADD CONSTRAINT {symbol.lower()}_1d_pkey PRIMARY KEY ("time");
                    END IF;
                END;
                $$;
            """))
    except Exception as e:
        logging.error(f"❌ Lỗi khi đảm bảo bảng/PK cho {symbol}_1D: {e}")



def get_foreign_symbol_today(symbol):
    try:
        data = pd.read_sql(
            text("""SELECT "foreignBuyVol","foreignSellVol","foreignBuyVal","foreignSellVal"
                    FROM details.asset
                    WHERE symbol = :symbol
                    LIMIT 1"""),
            con=enginedb,
            params={"symbol": symbol.strip().upper()}
        )
        if data.empty:
            msg = f"⚠️ Không có dữ liệu cho {symbol}, bỏ qua."
            logging.warning(msg)
            return [msg]
        
        netvol=data['foreignBuyVol'].iloc[0]-data['foreignSellVol'].iloc[0]
        netval=data['foreignBuyVal'].iloc[0]-data['foreignSellVal'].iloc[0]
            
        time=date.today().strftime('%Y-%m-%d')
        row = {"symbol": symbol,
               "time": time,
               "netVol": netvol,
               "netVal": netval
               }
        ensure_table(symbol)
        
        with enginedb.begin() as cur:
            cur.execute(text(f"""
                INSERT INTO asset_foreign_history."{symbol}_1D"
                ("symbol", "time", "netVol", "netVal")
                VALUES (:symbol, :time, :netVol, :netVal)
                ON CONFLICT ("time") DO UPDATE SET
                    "netVol" = EXCLUDED."netVol",
                    "netVal" = EXCLUDED."netVal";
            """), row)
        msg = f"✅ Đã upsert dữ liệu {symbol} ngày {time} vào asset_foreign_history.{symbol}_1D"
        logging.info(msg)
        return [msg]
    except Exception as e:
        msg = f"❌ Lỗi lưu {symbol}: {e}"
        logging.exception(msg)
        return [msg]

def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_foreign_symbol_today, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result() or []  # luôn là list
            messages.extend(result)
    return messages

def save_all_foreign_today():
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