from sqlalchemy import create_engine, text
import pandas as pd
import logging
import concurrent.futures
import re

from .event import get_event
from utils.create_list.symbol_list import total_list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

enginedb = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

SCHEMA = "stock_event"

# ---------- helpers ----------
def _safe_ident(name: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+$", name):
        raise ValueError(f"Invalid symbol for table name: {name}")
    return name

def table_exists(symbol: str) -> bool:
    sym = _safe_ident(symbol)
    full_name = f'{SCHEMA}."{sym}"'
    with enginedb.connect() as conn:
        return conn.execute(text("SELECT to_regclass(:n)"), {"n": full_name}).scalar() is not None

def ensure_schema_and_table(symbol: str):
    sym = _safe_ident(symbol)
    with enginedb.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{sym}" (
                "symbol" TEXT,
                "time"   DATE,
                "event"  TEXT,
                "label"  TEXT
            );
        '''))
        conn.execute(text(f'''
            ALTER TABLE "{SCHEMA}"."{sym}"
            ADD COLUMN IF NOT EXISTS "symbol" TEXT,
            ADD COLUMN IF NOT EXISTS "time" DATE,
            ADD COLUMN IF NOT EXISTS "event" TEXT,
            ADD COLUMN IF NOT EXISTS "label" TEXT;
        '''))

        # thử tạo unique index
        try:
            conn.execute(text(f'''
                CREATE UNIQUE INDEX IF NOT EXISTS "{sym}_uniq_time_event"
                ON "{SCHEMA}"."{sym}" ("time", "event");
            '''))
        except Exception as e:
            # nếu fail do duplicates -> dedupe rồi tạo lại
            msg = str(e).lower()
            if "duplicate" in msg or "could not create unique index" in msg:
                conn.execute(text(f'''
                    DELETE FROM "{SCHEMA}"."{sym}" a
                    USING "{SCHEMA}"."{sym}" b
                    WHERE a.ctid < b.ctid
                      AND a."time" = b."time"
                      AND a."event" = b."event";
                '''))
                conn.execute(text(f'''
                    CREATE UNIQUE INDEX IF NOT EXISTS "{sym}_uniq_time_event"
                    ON "{SCHEMA}"."{sym}" ("time", "event");
                '''))
            else:
                raise


def normalize_event_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["symbol", "time", "event", "label"])

    for c in ["symbol", "time", "event", "label"]:
        if c not in df.columns:
            df[c] = None

    df = df[["symbol", "time", "event", "label"]].copy()
    df["symbol"] = symbol
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.date
    df = df.dropna(subset=["time"])
    return df

# ---------- main worker ----------
def stock_event(symbol):
    try:
        sym = _safe_ident(symbol)

        # (1) check bảng -> (2) chưa có thì tạo
        ensure_schema_and_table(sym)

        # (3) gọi API đúng 1 lần (không retry)
        try:
            data = get_event(sym)
        except Exception as e:
            msg = f"❌ {sym}: lỗi get_event - {e}"
            log.error(msg, exc_info=True)
            return msg

        data = normalize_event_df(data, sym)

        # (5) không có data -> thôi
        if data.empty:
            msg = f"⚠️ {sym}: API không có dữ liệu, không add gì."
            log.info(msg)
            return msg

        # (4) có data -> ADD (append) vào bảng, trùng thì bỏ qua
        rows = data.to_dict("records")
        insert_sql = text(f'''
            INSERT INTO "{SCHEMA}"."{sym}" ("symbol","time","event","label")
            VALUES (:symbol,:time,:event,:label)
            ON CONFLICT ("time","event") DO NOTHING;
        ''')
        with enginedb.begin() as conn:
            conn.execute(insert_sql, rows)

        msg = f"✅ {sym}: add {len(rows)} rows (trùng sẽ bỏ qua)."
        log.info(msg)
        return msg

    except Exception as e:
        msg = f"❌ {symbol}: lỗi khi lưu - {e}"
        log.error(msg, exc_info=True)
        return msg

def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(stock_event, sym): sym for sym in symbol_list}
        for future in concurrent.futures.as_completed(futures):
            sym = futures[future]
            try:
                msg = future.result()
                if msg:
                    messages.append(msg)
            except Exception as e:
                err_msg = f"❌ {sym}: task bị lỗi không mong muốn - {e}"
                log.error(err_msg, exc_info=True)
                messages.append(err_msg)
    return messages

def save_stock_event():
    log.info("🚀 Bắt đầu cập nhật dữ liệu...")
    result = update_all_stocks(total_list)

    errors = [m for m in result if m.startswith("❌")]
    skipped = [m for m in result if m.startswith("⚠️")]
    success = [m for m in result if m.startswith("✅")]

    log.info(f"📊 Tổng số mã xử lý: {len(result)}")
    log.info(f"✅ Thành công: {len(success)} | ⚠️ Không có data: {len(skipped)} | ❌ Lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    enginedb.dispose()
    return result
