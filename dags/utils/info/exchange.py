import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from utils.create_list.symbol_list import total_list, EXCHANGE_LISTS, HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE
from utils.create_list.indices_map import indices_map

logger = logging.getLogger(__name__)

# ---------- lookup tables ----------
_EX = {s.strip().upper(): ex for ex, lst in EXCHANGE_LISTS.items() for s in lst}

_IDX = {}
for idx, syms in indices_map.items():
    for s in syms:
        _IDX.setdefault(s.strip().upper(), []).append(idx)
_IDX = {s: "|".join(v) for s, v in _IDX.items()}

_TYPE = {
    s.strip().upper(): t
    for lst, t in [
        (HOSE, "Stock"), (HNX, "Stock"), (UPCOM, "Stock"),
        (DERIVATIVES, "Derivative"), (CW, "Warrant"),
        (HNXBOND, "Bond"), (ETFHOSE, "ETF"),
    ]
    for s in lst
}

# ---------- DB ----------
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
    pool_pre_ping=True, pool_size=10, max_overflow=20,
)
_table = Table("asset", MetaData(), schema="info", autoload_with=engine)


def _upsert(rows):
    if not rows:
        return 0
    stmt = pg_insert(_table).values(rows)
    upd = {k: getattr(stmt.excluded, k) for k in ("exchange", "indices", "type") if k in _table.c}
    with engine.begin() as conn:
        conn.execute(stmt.on_conflict_do_update(index_elements=[_table.c.symbol], set_=upd))
    return len(rows)


def _build(syms):
    return [{"symbol": s, "exchange": _EX.get(s), "indices": _IDX.get(s), "type": _TYPE.get(s)}
            for s in (s.strip().upper() for s in syms)]


def update_exchange(chunk_size=300, max_workers=8):
    syms = [s.strip().upper() for s in total_list]
    chunks = [syms[i:i+chunk_size] for i in range(0, len(syms), chunk_size)]
    workers = min(max_workers, len(chunks))

    logger.info("Start: %d symbols, %d chunks, %d workers", len(syms), len(chunks), workers)

    total = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for r in as_completed([ex.submit(_upsert, _build(c)) for c in chunks]):
            total += r.result()

    logger.info("Done: %d symbols upserted", total)
    return total


if __name__ == "__main__":
    update_exchange()