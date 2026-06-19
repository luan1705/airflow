from sqlalchemy import create_engine, text

def active_asset():
    engine = create_engine(
        "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
    )

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE info.asset
                SET active = FALSE;
            """))
    finally:
        engine.dispose()