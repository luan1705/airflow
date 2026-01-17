from sqlalchemy import create_engine, text

DB_URL = "postgresql://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"

engine = create_engine(DB_URL, pool_pre_ping=True)

UPSERT_SQL = """
with universe as (
    select
        symbol,
        industry,
        "marketCap"
    from info.asset
    where "marketCap" is not null
      and industry is not null
),
total_market as (
    select
        sum("marketCap") as total_market_cap
    from universe
),
industry_total as (
    select
        industry as industry_key,
        sum("marketCap") as total_industry_cap
    from universe
    group by industry
),
calculated as (
    select
        u.symbol,
        u."marketCap" / tm.total_market_cap     as "marketWeight",
        u."marketCap" / it.total_industry_cap   as "industryWeight"
    from universe u
    cross join total_market tm
    join industry_total it
        on u.industry = it.industry_key
)
insert into info.asset (
    symbol,
    "marketWeight",
    "industryWeight"
)
select
    symbol,
    "marketWeight",
    "industryWeight"
from calculated
on conflict (symbol) do update
set
    "marketWeight"   = excluded."marketWeight",
    "industryWeight" = excluded."industryWeight";
"""

def main():
    with engine.begin() as conn:
        conn.execute(text(UPSERT_SQL))

    print("✅ marketWeight & industryWeight upserted successfully")

if __name__ == "__main__":
    main()
