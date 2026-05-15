import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
SCHEMA = "exchange_history"

EXCHANGES = {
    "HOSE": "%VNINDEX%",
    "HNX": "%HNXINDEX%",
    "UPCOM": "%UPCOMINDEX%",
}
CONDITIONS = {
    "EMA20": "ema20",
    "EMA50": "ema50",
    "EMA100": "ema100",
    "EMA200": "ema200",
}


def breadth_today(db_url=DB_URL, schema=SCHEMA, today=None):
    today = pd.to_datetime(today).date() if today else pd.Timestamp.today().date()
    engine = create_engine(db_url)
    rows = []

    try:
        col_sql = text(
            '''\
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='status' AND table_name='alert_status'
            '''
        )
        available_cols = set(pd.read_sql(col_sql, engine)["column_name"].tolist())

        for exchange, index_like in EXCHANGES.items():
            total_sql = text(
                '''\
                SELECT COUNT(*) AS total
                FROM info.asset i
                WHERE i.indices LIKE :index_like
                  AND i.available IS TRUE
                '''
            )
            total = int(pd.read_sql(total_sql, engine, params={"index_like": index_like}).iloc[0]["total"])

            for condition, ema_col in CONDITIONS.items():
                period = condition.replace("EMA", "")
                table_name = f"breadth{period}_{exchange}"

                if ema_col not in available_cols:
                    count = None
                    percent = None
                    status = f"upserted_missing_column:{ema_col}"
                else:
                    count_sql = text(
                        f'''\
                        SELECT COUNT(*) AS count
                        FROM status.alert_status s
                        LEFT JOIN info.asset i USING (symbol)
                        WHERE s.close > s.{ema_col}
                          AND s.time::date = CURRENT_DATE
                          AND i.indices LIKE :index_like
                          AND i.available IS TRUE
                        '''
                    )
                    count = int(pd.read_sql(count_sql, engine, params={"index_like": index_like}).iloc[0]["count"])
                    percent = None if total == 0 else count / total
                    status = "upserted"

                upsert_sql = text(
                    f'''\
                    INSERT INTO "{schema}"."{table_name}" ("date", "condition", "count", "total", "percent")
                    VALUES (:date, :condition, :count, :total, :percent)
                    ON CONFLICT ("date") DO UPDATE SET
                        "condition" = EXCLUDED."condition",
                        "count" = EXCLUDED."count",
                        "total" = EXCLUDED."total",
                        "percent" = EXCLUDED."percent"
                    '''
                )
                with engine.begin() as conn:
                    conn.execute(
                        upsert_sql,
                        {
                            "date": today,
                            "condition": condition,
                            "count": None if count is None else int(count),
                            "total": int(total),
                            "percent": None if percent is None else float(percent),
                        },
                    )

                rows.append(
                    {
                        "date": today,
                        "exchange": exchange,
                        "condition": condition,
                        "count": None if count is None else int(count),
                        "total": int(total),
                        "percent": percent,
                        "table": table_name,
                        "status": status,
                    }
                )

        return pd.DataFrame(rows).sort_values(["exchange", "condition"]).reset_index(drop=True)
    finally:
        engine.dispose()
