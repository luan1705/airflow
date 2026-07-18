from sqlalchemy import create_engine, text
import pandas as pd

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"


def upsert_volatility_history():
    engine = create_engine(DB_URL)

    query = """
    SELECT *
    FROM details.exchange_volatility
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        print("Không có dữ liệu trong details.exchange_volatility")
        return

    df["date"] = pd.Timestamp.today().date()
    df = df[["exchange", "date", "advancersVal", "noChangesVal", "declinersVal"]]

    table_map = {
        "HOSE": "volatility_HOSE",
        "HNX": "volatility_HNX",
        "UPCOM": "volatility_UPCOM",
    }

    total_rows = 0

    with engine.begin() as conn:
        for exchange, table_name in table_map.items():
            df_exchange = df[df["exchange"] == exchange].copy()

            if df_exchange.empty:
                print(f"Không có dữ liệu cho sàn {exchange}")
                continue

            df_exchange = df_exchange[["date", "advancersVal", "noChangesVal", "declinersVal"]]
            records = df_exchange.to_dict(orient="records")

            upsert_sql = text(f"""
                INSERT INTO exchange_history."{table_name}" (
                    date, "advancersVal", "noChangesVal", "declinersVal"
                )
                VALUES (
                    :date, :advancersVal, :noChangesVal, :declinersVal
                )
                ON CONFLICT (date)
                DO UPDATE SET
                    "advancersVal" = EXCLUDED."advancersVal",
                    "noChangesVal" = EXCLUDED."noChangesVal",
                    "declinersVal" = EXCLUDED."declinersVal"
            """)

            conn.execute(upsert_sql, records)
            total_rows += len(df_exchange)

            print(f"Upsert thành công {len(df_exchange)} dòng vào exchange_history.{table_name}")

    print(f"Tổng cộng upsert thành công {total_rows} dòng")