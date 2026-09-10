from sqlalchemy import create_engine, text
from db_config import POSTGRES_URL

def active_asset():
    engine = create_engine(
        POSTGRES_URL,
        poolclass=NullPool
    )

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE info.asset
                SET active = FALSE;
            """))
    finally:
        engine.dispose()