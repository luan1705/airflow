from sqlalchemy import create_engine, text
import pandas as pd
import logging
from datetime import timedelta
import concurrent.futures
import re

from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW
from .def_asset_proprietary_history import proprietary_history

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

enginedb = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

TARGET_SCHEMA = "asset_proprietary_history"  # đúng theo bạn yêu cầu

def safe_table_name(symbol: str) -> str:
    """
    Chỉ cho phép A-Z0-9_ để tránh SQL injection qua tên bảng.
    """
    sym = (symbol or "").upper().strip()
    if not re.fullmatch(r"[A-Z0-9_]+", sym):
        raise ValueError(f"Invalid symbol for table name: {symbol}")
    return sym

def ensure_schema_and_table(conn, table_name: str):
    # tạo schema nếu chưa có
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}"'))

    # tạo bảng theo symbol nếu chưa có
    # PK = time (mỗi ngày 1 dòng). Nếu bạn muốn nhiều dòng/ngày thì đổi PK.
    conn.execute(text(f'''
        CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."{table_name}_1D" (
            "symbol" text,
            "time" TIMESTAMPTZ PRIMARY KEY,
            "netVol" bigint,
            "netVal" bigint
        );
    '''))

def get_latest_time_from_source():
    # LẤY 1 GIÁ TRỊ MAX(time) thay vì SELECT *
    with enginedb.connect() as conn:
        r = conn.execute(text('SELECT max("time") AS max_time FROM "exchange_history"."proprietary_HOSE_1D"'))
        max_time = r.scalar()
    return max_time  # có thể là date / datetime / None

def get_proprietary_history(symbol):
    try:
        # 1) lấy ngày mới nhất (chỉ 1 cell)
        ngay_moi_nhat = get_latest_time_from_source()
        if ngay_moi_nhat is None:
            msg = f"❌ Không lấy được max(time) từ proprietary_HOSE_1D"
            log.error(msg)
            return [msg]

        # chuẩn hóa về date
        ngay_moi_nhat = pd.to_datetime(ngay_moi_nhat).date()
        ngay_bat_dau = ngay_moi_nhat - timedelta(days=100)
        # ngày chỉ định
        # ngay_moi_nhat = "2026-05-12"
        # ngay_bat_dau = "2023-01-01"

        # 2) gọi API lấy 15 ngày
        data = proprietary_history(
            symbol,
            time="days",
            start=str(ngay_bat_dau),   # 'YYYY-MM-DD'
            end=str(ngay_moi_nhat)
            # ngày chỉ định
            # start=ngay_bat_dau,
            # end=ngay_moi_nhat
        )

        table_name = safe_table_name(symbol)
        with enginedb.begin() as conn:
            ensure_schema_and_table(conn, table_name)

        if data.empty:
            msg = f"⚠️ Không có dữ liệu cho {symbol}, bỏ qua."
            log.info(msg)
            return [msg]

        # 3) map về schema lưu
        col = ['Mã CP', 'Thời điểm GD', 'Tổng KLGD ròng', 'Tổng GTGD ròng']
        data = data[col].copy()
        data.columns = ['symbol', 'time', 'netVol', 'netVal']

        data["time"] = pd.to_datetime(data["time"]).dt.tz_localize("Asia/Ho_Chi_Minh")
        data["netVol"] = pd.to_numeric(data["netVol"], errors="coerce").fillna(0).astype("int64")
        data["netVal"] = pd.to_numeric(data["netVal"], errors="coerce").fillna(0).astype("int64")

        # 4) lưu vào bảng theo symbol
        rows = data[["symbol", "time", "netVol", "netVal"]].to_dict("records")

        with enginedb.begin() as conn:
            # upsert theo time (PK)
            conn.execute(
                text(f'''
                    INSERT INTO "{TARGET_SCHEMA}"."{table_name}_1D" ("symbol", "time", "netVol", "netVal")
                    VALUES (:symbol, :time, :netVol, :netVal)
                    ON CONFLICT ("time") DO UPDATE SET
                    "symbol" = EXCLUDED."symbol",
                    "netVol" = EXCLUDED."netVol",
                    "netVal" = EXCLUDED."netVal";
                '''),
                rows
            )  # executemany

        msg = f"✅ Upsert {symbol} -> {TARGET_SCHEMA}.{table_name}_1D ({len(rows)} dòng) OK"
        log.info(msg)
        return [msg]

    except Exception as e:
        msg = f"❌ Lỗi {symbol}: {e}"
        log.exception(msg)
        return [msg]

def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(get_proprietary_history, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.extend(future.result() or [])
    return messages

def save_proprietary_history():
    log.info("🚀 Bắt đầu cập nhật dữ liệu...")
    result = []
    result += update_all_stocks(HOSE)
    result += update_all_stocks(HNX)
    result += update_all_stocks(UPCOM)
    result += update_all_stocks(DERIVATIVES)
    result += update_all_stocks(CW)

    errors = [msg for msg in result if msg.startswith("❌")]
    warnings = [msg for msg in result if msg.startswith("⚠️")]

    log.info(f"📊 Tổng task trả về: {len(result)}")
    log.info(f"❌ Lỗi: {len(errors)} | ⚠️ Không có dữ liệu: {len(warnings)}")

    if errors:
        log.warning("📛 Chi tiết lỗi:")
        for err in errors:
            log.warning(err)

    log.info("🎉 Hoàn thành.")
    enginedb.dispose()
    return errors if errors else ["✅ Không có lỗi"]
