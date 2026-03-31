import psycopg2
from psycopg2.extras import execute_values
from utils.create_list.indices_map import indices_map
from datetime import timedelta

DB_URL = "postgresql://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"

SRC_SCHEMA = "asset_foreign_history"
TARGET_SCHEMA = "indices_foreign_history"   # 👈 schema mới
TIMEFRAME = "1D"


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def ensure_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")


def ensure_target_table(cur, target_table: str):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}."{target_table}" (
          "symbol" text NOT NULL,
          "time" timestamptz PRIMARY KEY,
          "netVol" bigint,
          "netVal" bigint,
          "n_symbols" integer
        )
        """
    )


def get_refresh_from_time(cur, target_table: str):
    cur.execute(f'SELECT max("time") FROM {TARGET_SCHEMA}."{target_table}"')
    last_time = cur.fetchone()[0]
    if last_time is None:
        return None
    return last_time - timedelta(days=7)


def build_agg_sql(existing_tables: list[str], refresh_from_time):
    parts = []
    for t in existing_tables:
        sym = t.rsplit("_", 1)[0]
        if refresh_from_time:
            parts.append(
                f"""SELECT '{sym}'::text AS src_symbol, "time","netVol","netVal"
                    FROM {SRC_SCHEMA}."{t}"
                    WHERE "time" > %s"""
            )
        else:
            parts.append(
                f"""SELECT '{sym}'::text AS src_symbol, "time","netVol","netVal"
                    FROM {SRC_SCHEMA}."{t}" """
            )

    union_sql = "\nUNION ALL\n".join(parts)

    return f"""
    WITH u AS (
      {union_sql}
    )
    SELECT
      "time",
      SUM("netVol")::bigint AS "netVol",
      SUM("netVal")::bigint AS "netVal",
      COUNT(DISTINCT src_symbol)::int AS "n_symbols"
    FROM u
    GROUP BY "time"
    ORDER BY "time"
    """


def upsert(cur, target_table: str, rows):
    if not rows:
        return
    sql = f"""
    INSERT INTO {TARGET_SCHEMA}."{target_table}" ("symbol","time","netVol","netVal","n_symbols")
    VALUES %s
    ON CONFLICT ("time")
    DO UPDATE SET
      "symbol" = EXCLUDED."symbol",
      "netVol" = EXCLUDED."netVol",
      "netVal" = EXCLUDED."netVal",
      "n_symbols" = EXCLUDED."n_symbols"
    """
    execute_values(cur, sql, rows, page_size=2000)


def is_derivative_group(index_name: str) -> bool:
    return index_name.upper().startswith("FUTURE")


def aggregate_one_index(cur, index_name: str, symbols: list[str]):
    target_table = f"{index_name}_{TIMEFRAME}"

    ensure_target_table(cur, target_table)

    candidate_tables = [f"{sym}_{TIMEFRAME}" for sym in symbols]

    existing_tables = []
    for t in candidate_tables:
        if table_exists(cur, SRC_SCHEMA, t):
            existing_tables.append(t)

    if not existing_tables:
        print(f"[{index_name}] no source tables found -> skip")
        return

    refresh_from_time = get_refresh_from_time(cur, target_table)
    agg_sql = build_agg_sql(existing_tables, refresh_from_time)

    if refresh_from_time:
        cur.execute(agg_sql, (refresh_from_time,) * len(existing_tables))
    else:
        cur.execute(agg_sql)

    agg_rows = cur.fetchall()

    rows = [(index_name, r[0], r[1], r[2], r[3]) for r in agg_rows]

    upsert(cur, target_table, rows)

    print(f"[{index_name}] rows={len(rows)} refresh_from={refresh_from_time}")


def run_all_indices():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False

    with conn.cursor() as cur:
        ensure_schema(cur)  # 👈 đảm bảo schema tồn tại

        for index_name, symbols in indices_map.items():
            if not isinstance(symbols, list) or not symbols:
                continue
            if is_derivative_group(index_name):
                continue

            try:
                aggregate_one_index(cur, index_name, symbols)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"[{index_name}] FAIL -> {e}")

    conn.close()
    print("DONE.")


# if __name__ == "__main__":
#     run_all_indices()