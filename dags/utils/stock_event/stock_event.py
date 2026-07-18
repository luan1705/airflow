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
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

SCHEMA = "stock_event"


def _safe_ident(name: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+$", name):
        raise ValueError(f"Invalid symbol for table name: {name}")
    return name

def ensure_schema():
    with enginedb.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'))


def ensure_schema_and_table(symbol: str):
    sym = _safe_ident(symbol)
    with enginedb.begin() as conn:
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{sym}" (
                "symbol"        TEXT,
                "time"          DATE,
                "label"         TEXT,
                "valuePerShare" DOUBLE PRECISION,
                "exerciseRate"  DOUBLE PRECISION,
                "issueVolume"   DOUBLE PRECISION,
                "exerciseRatio" DOUBLE PRECISION,
                "revenue"       DOUBLE PRECISION,
                "profit"        DOUBLE PRECISION,
                "lengthReport"  DOUBLE PRECISION,
                "yearReport"    DOUBLE PRECISION
            );
        '''))
        for col, dtype in [
            ("symbol", "TEXT"), ("time", "DATE"), ("label", "TEXT"),
            ("valuePerShare", "DOUBLE PRECISION"), ("exerciseRate", "DOUBLE PRECISION"),
            ("issueVolume", "DOUBLE PRECISION"), ("exerciseRatio", "DOUBLE PRECISION"),
            ("revenue", "DOUBLE PRECISION"), ("profit", "DOUBLE PRECISION"),
            ("lengthReport", "DOUBLE PRECISION"), ("yearReport", "DOUBLE PRECISION"),
        ]:
            conn.execute(text(f'ALTER TABLE "{SCHEMA}"."{sym}" ADD COLUMN IF NOT EXISTS "{col}" {dtype};'))

        try:
            conn.execute(text(f'''
                CREATE UNIQUE INDEX IF NOT EXISTS "{sym}_uniq_time_label"
                ON "{SCHEMA}"."{sym}" ("time", "label");
            '''))
        except Exception as e:
            msg = str(e).lower()
            if "duplicate" in msg or "could not create unique index" in msg:
                conn.execute(text(f'''
                    DELETE FROM "{SCHEMA}"."{sym}" a
                    USING "{SCHEMA}"."{sym}" b
                    WHERE a.ctid < b.ctid
                      AND a."time" = b."time"
                      AND a."label" = b."label"
                '''))
                conn.execute(text(f'''
                    CREATE UNIQUE INDEX IF NOT EXISTS "{sym}_uniq_time_label"
                    ON "{SCHEMA}"."{sym}" ("time", "label");
                '''))
            else:
                raise


def normalize_event_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=['symbol', 'time', 'label', 'valuePerShare', 'exerciseRate', 'issueVolume', 'exerciseRatio', 'revenue', 'profit', 'lengthReport', 'yearReport'])

    cols = ['symbol', 'time', 'label', 'valuePerShare', 'exerciseRate', 'issueVolume', 'exerciseRatio', 'revenue', 'profit', 'lengthReport', 'yearReport']
    for c in cols:
        if c not in df.columns:
            df[c] = None

    df = df[cols].copy()
    df['symbol'] = symbol
    df['time'] = pd.to_datetime(df['time'], errors='coerce').dt.date
    df = df.dropna(subset=['time'])
    return df


def stock_event(symbol):
    try:
        sym = _safe_ident(symbol)
        ensure_schema_and_table(sym)

        try:
            data = get_event(sym)
        except Exception as e:
            msg = f"❌ {sym}: lỗi get_event - {e}"
            log.error(msg, exc_info=True)
            return msg

        data = normalize_event_df(data, sym)

        if data.empty:
            msg = f"⚠️ {sym}: API không có dữ liệu, không add gì."
            log.info(msg)
            return msg

        rows = data.to_dict('records')
        for row in rows:
            for k, v in row.items():
                try:
                    if pd.isna(v):
                        row[k] = None
                except Exception:
                    pass
                if hasattr(v, 'item'):
                    row[k] = v.item()

        insert_sql = text(f'''
            INSERT INTO "{SCHEMA}"."{sym}"
                ("symbol","time","label","valuePerShare","exerciseRate","issueVolume","exerciseRatio","revenue","profit","lengthReport","yearReport")
            VALUES
                (:symbol,:time,:label,:valuePerShare,:exerciseRate,:issueVolume,:exerciseRatio,:revenue,:profit,:lengthReport,:yearReport)
            ON CONFLICT ("time","label") DO NOTHING;
        ''')
        with enginedb.begin() as conn:
            conn.execute(insert_sql, rows)

        msg = f"✅ {sym}: add {len(rows)} rows."
        log.info(msg)
        return msg

    except Exception as e:
        msg = f"❌ {symbol}: lỗi khi lưu - {e}"
        log.error(msg, exc_info=True)
        return msg


def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
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
    ensure_schema()
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
    return errors