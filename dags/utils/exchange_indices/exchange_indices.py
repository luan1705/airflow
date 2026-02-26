import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

from utils.create_list.symbol_list import total_list, EXCHANGE_LISTS
from utils.create_list.indices_map import indices_map

# ---------- LOG ----------
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- build lookup ----------
_SYMBOL2EX = {s.strip().upper(): ex for ex, lst in EXCHANGE_LISTS.items() for s in lst}

_SYM2IDX = {}
for idx, symbols in indices_map.items():
    for s in symbols:
        _SYM2IDX.setdefault(s.strip().upper(), []).append(idx)
_SYM2IDX = {s: "|".join(v) for s, v in _SYM2IDX.items()}

def get_exchange(sym: str):
    return _SYMBOL2EX.get(sym.strip().upper()) if sym else None

def get_indices(sym: str):
    return _SYM2IDX.get(sym.strip().upper()) if sym else None

# ---------- DB engine ----------
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)
metadata = MetaData()
eboard = Table("asset", metadata, schema="info", autoload_with=engine)

# ---------- helpers ----------
def chunked(lst, size=300):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def upsert_eboard_many(rows):
    if not rows:
        return 0

    stmt = pg_insert(eboard).values(rows)
    upd = {k: getattr(stmt.excluded, k) for k in ("exchange", "indices") if k in eboard.c}

    with engine.begin() as conn:
        conn.execute(
            stmt.on_conflict_do_update(
                index_elements=[eboard.c.symbol],
                set_=upd
            )
        )
    return len(rows)

def build_rows(symbols):
    rows = []
    for sym in symbols:
        s = sym.strip().upper()
        rows.append({
            "symbol": s,
            "exchange": get_exchange(s),
            "indices": get_indices(s),  # "x|y|z" hoặc None
        })
    return rows

def worker(sym_chunk):
    return upsert_eboard_many(build_rows(sym_chunk))

# ---------- RUN (đưa vào function để gọi từ Airflow) ----------
def upsert_ex_in(chunk_size=300, max_workers=8) -> int:
    all_syms = [s.strip().upper() for s in total_list]
    chunks = list(chunked(all_syms, size=chunk_size))

    total = 0
    workers = min(max_workers, len(chunks)) if chunks else 0

    logger.info("Start upsert: symbols=%d chunks=%d chunk_size=%d workers=%d",
                len(all_syms), len(chunks), chunk_size, workers)

    if workers == 0:
        logger.info("No symbols to upsert.")
        return 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(worker, c) for c in chunks]
        for f in as_completed(futs):
            total += f.result()

    logger.info("Done upsert: %d symbols", total)
    return total

