from sqlalchemy import create_engine, text

def active_asset():
    engine = create_engine(
        "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
    )

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE info.asset
                SET active = FALSE;
            """))
    finally:
        engine.dispose()