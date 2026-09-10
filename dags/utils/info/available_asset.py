from sqlalchemy import create_engine, text
from utils.create_list.symbol_list import total_list as keep
from db_config import POSTGRES_URL

def available_asset():
    engine = create_engine(POSTGRES_URL, poolclass=NullPool)
    sql = text("""
    UPDATE info.asset
    SET available = COALESCE(symbol = ANY(CAST(:keep AS text[])), FALSE)
    """)
    try:
        with engine.begin() as conn:
            conn.execute(sql, {"keep": keep})
    finally:
        engine.dispose()  # đóng pool, trả hết connections