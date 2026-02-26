from sqlalchemy import create_engine , MetaData, Table 
import concurrent.futures
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from utils.create_list.symbol_list import indices
import time
import logging
from .tradingview import tradingview

# Thiết lập logging 
logging.basicConfig(
    level=logging.INFO,  # mức log tối thiểu (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s [%(levelname)s] %(message)s",  # định dạng log
    handlers=[
        logging.StreamHandler(),  # in ra console
        logging.FileHandler("stock_update.log", encoding="utf-8")  # lưu vào file
    ]
)
log=logging.getLogger(__name__)

# Kết nối PostgreSQL
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
    pool_size=10, max_overflow=20, pool_timeout=60,
    pool_pre_ping=True, pool_recycle=1800,
    connect_args={"keepalives":1, "keepalives_idle":30, "keepalives_interval":10, "keepalives_count":3},
)

metadata = MetaData()
mi_table = Table("vietnam", metadata, schema="indices", autoload_with=engine)

def update_mi(row: dict) -> int:
    """Chỉ UPDATE các cột có trong row (trừ 'symbol'). Trả về số dòng ảnh hưởng."""
    if not row or "symbol" not in row:
        return 0
    values = {k: v for k, v in row.items() if k != "symbol"}
    if not values:
        return 0
    stmt = mi_table.update().where(mi_table.c.symbol == row["symbol"]).values(**values)
    with engine.begin() as conn:
        conn.exec_driver_sql("SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='5s'")
        res = conn.execute(stmt)
        return res.rowcount or 0

# Hàm cập nhật dữ liệu cho một mã cổ phiếu
def get_stock(symbol, max_retries=3, base_delay=2):
    symbol = 'UPCOMINDEX' if symbol == 'HNXUpcomIndex' else symbol
    symbol = 'HNXINDEX' if symbol == 'HNXIndex' else symbol
    attempt = 0
    while attempt < max_retries:
        try:
            time.sleep(2)
            today = datetime.now().strftime('%Y-%m-%d')

            # Lấy dữ liệu lịch sử
            stock = tradingview(symbol=symbol)

            # Kiểm tra dữ liệu trả về
            if stock is None or stock.empty:
                msg = f"⚠️ Không có dữ liệu cho {symbol}"
                log.warning(msg)
                return msg

            # Update vào PostgreSQL
            rec = stock.squeeze()  # DF 1 dòng -> Series

            row = {
                "symbol": symbol,
                "open":  float(rec.get("open"))  if rec.get("open")  is not None else None,
                "high":  float(rec.get("high"))  if rec.get("high")  is not None else None,
                "low":   float(rec.get("low"))   if rec.get("low")   is not None else None,
            }

            # Sau 15:00 (giờ VN) mới cập nhật close
            local_now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
            if local_now.time() >= dtime(15, 0):
                row["close"] = float(rec.get("close")) if rec.get("close") is not None else None

            affected = update_mi(row)
            if affected > 0:
                msg = f"✅ Đã update {symbol}"
                log.info(msg)
            else:
                msg = f"ℹ️ Không có dòng để update cho {symbol} (chưa tồn tại)"
                log.info(msg)
            return msg

        except Exception as e:
            attempt += 1
            msg = f"❌ Lỗi khi xử lý {symbol}: {str(e)}"
            log.error(msg)
            
            if attempt < max_retries:
                delay = base_delay * attempt  # delay tăng dần: 2s, 4s, 6s...
                log.info(f"🔄 Thử lại {symbol} sau {delay}s...")
                time.sleep(delay)
            else:
                return msg

# Cập nhật toàn bộ danh sách mã cho một sàn
def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_stock, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.append(future.result())
    return messages


# Hàm tổng cho tất cả sàn — để DAG gọi
def save_olch():
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    result = []
    result += update_all_stocks(indices)

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

if __name__ == "__main__":
    # Chạy hàm save_olch để cập nhật dữ liệu
    save_olch()
    print("✅ Hoàn thành cập nhật dữ liệu OLCH.")