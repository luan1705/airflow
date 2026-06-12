import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.float64, lambda v: AsIs(float(v)))
register_adapter(np.int64,   lambda v: AsIs(int(v)))


def invest_capital(n_days=3):
    url = "https://api-feature.sstock.vn/api/v1/chart/general-data-series?dataSeriesNames=T%E1%BB%B1+doanh+r%C3%B2ng&dataSeriesNames=C%C3%A1+nh%C3%A2n+trong+n%C6%B0%E1%BB%9Bc+r%C3%B2ng&dataSeriesNames=T%E1%BB%95+ch%E1%BB%A9c+trong+n%C6%B0%E1%BB%9Bc+r%C3%B2ng&dataSeriesNames=C%C3%A1+nh%C3%A2n+n%C6%B0%E1%BB%9Bc+ngo%C3%A0i+r%C3%B2ng&dataSeriesNames=T%E1%BB%95+ch%E1%BB%A9c+n%C6%B0%E1%BB%9Bc+ngo%C3%A0i+r%C3%B2ng"
    headers = {'Cookie': '__Secure-better-auth.session_token=67ahD0YRN0VvUfQFJTZ66G2dU99QkO8S.0Mrc0z5D1n7DP%2Bjmv1bSTqIcKwi3ZteatV94FjLWTOo%3D; __Secure-better-auth.session_data=eyJzZXNzaW9uIjp7InNlc3Npb24iOnsiZXhwaXJlc0F0IjoiMjAyNi0wNi0xNVQwNzoyODowNS41NDZaIiwidG9rZW4iOiI2N2FoRDBZUk4wVnZVZlFGSlRaNjZHMmRVOTlRa084UyIsImNyZWF0ZWRBdCI6IjIwMjYtMDYtMDhUMDc6Mjg6MDUuNTQ2WiIsInVwZGF0ZWRBdCI6IjIwMjYtMDYtMDhUMDc6Mjg6MDUuNTQ2WiIsImlwQWRkcmVzcyI6IjEwLjQyLjAuMTQ3IiwidXNlckFnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzE0OC4wLjAuMCBTYWZhcmkvNTM3LjM2IiwidXNlcklkIjoiaUV5YTZXNEFQUzAyUWFibHRtR2o2M3JMWTRFeHJyZUIiLCJpbXBlcnNvbmF0ZWRCeSI6bnVsbCwiYWN0aXZlT3JnYW5pemF0aW9uSWQiOm51bGwsImFjdGl2ZVRlYW1JZCI6bnVsbCwiaWQiOiI5YWlWaHVvUjRBYnIwN0lURUIxSVcyR1dwV09qdzNBZCJ9LCJ1c2VyIjp7Im5hbWUiOiJQaGFuIExvaSIsImVtYWlsIjoiY3NvbGl1bG9AZ21haWwuY29tIiwiZW1haWxWZXJpZmllZCI6dHJ1ZSwiaW1hZ2UiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQ2c4b2NMaGFoNkYwa0JLckh1RmZsdG0wdnpKamZKWVJ1VFo2RWVYZ1VXSWdqWWlseDk1RGc9czk2LWMiLCJjcmVhdGVkQXQiOiIyMDI2LTA1LTI1VDAyOjU2OjU0LjM4N1oiLCJ1cGRhdGVkQXQiOiIyMDI2LTA1LTI1VDAyOjU2OjU0LjM4N1oiLCJ1c2VybmFtZSI6bnVsbCwiZGlzcGxheVVzZXJuYW1lIjpudWxsLCJyb2xlIjoidXNlciIsImJhbm5lZCI6ZmFsc2UsImJhblJlYXNvbiI6bnVsbCwiYmFuRXhwaXJlcyI6bnVsbCwidXNlclR5cGUiOm51bGwsImRpc3BsYXlQaG9uZU51bWJlciI6bnVsbCwiaWQiOiJpRXlhNlc0QVBTMDJRYWJsdG1HajYzckxZNEV4cnJlQiJ9LCJ1cGRhdGVkQXQiOjE3ODA5MDM2ODU1NzQsInZlcnNpb24iOiIxIn0sImV4cGlyZXNBdCI6MTc4MDkwMzk4NTU3NCwic2lnbmF0dXJlIjoiTC04Q0o4MDA4ZXpYMnFoY0JuZlFGcU9mUlJKWHMxRDdWOWI5ZHZQbEE3YyJ9; ph_phc_2O2eCgo6AOpwUykoQ5ufJGvaahcsg9cOPCMp4sZwSMh_posthog=%7B%22%24device_id%22%3A%220198d510-c557-74c1-98a3-4deb13835f48%22%2C%22distinct_id%22%3A%220198d510-c557-74c1-98a3-4deb13835f48%22%2C%22%24sesid%22%3A%5B1780903985308%2C%22019ea60a-2ff9-7412-a60f-d749c2504c20%22%2C1780902146032%5D%2C%22%24initial_person_info%22%3A%7B%22r%22%3A%22%24direct%22%2C%22u%22%3A%22https%3A%2F%2Fsstock.vn%2Ftong-quan-kenh-tai-san%22%7D%2C%22%24user_state%22%3A%22anonymous%22%7D'}
    data = requests.get(url, headers=headers).json()['dataSeriesValuesInfo']

    # gộp + pivot
    frames = []
    for name, records in data.items():
        tmp = pd.DataFrame(records)
        tmp['category'] = name
        frames.append(tmp)
    long = pd.concat(frames, ignore_index=True)
    long['date'] = pd.to_datetime(long['date'])

    table = (long.pivot(index='date', columns='category', values='value')
                 .sort_index(ascending=False)
                 .reset_index()
                 .rename_axis(columns=None)
                 .rename(columns={
                     'Tự doanh ròng': 'netProprietary',
                     'Cá nhân trong nước ròng': 'netDomesticIndividual',
                     'Tổ chức trong nước ròng': 'netDomesticInstitution',
                     'Cá nhân nước ngoài ròng': 'netForeignIndividual',
                     'Tổ chức nước ngoài ròng': 'netForeignInstitution',
                 }))

    value_cols = ['netProprietary', 'netDomesticIndividual', 'netDomesticInstitution',
                  'netForeignIndividual', 'netForeignInstitution']
    table[value_cols] = (table[value_cols] * 1_000_000_000).round(0)

    # DB
    engine = create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")

    with engine.begin() as con:
        con.execute(text("CREATE SCHEMA IF NOT EXISTS exchange_history"))
    with engine.begin() as con:
        con.execute(text('''
            CREATE TABLE IF NOT EXISTS exchange_history.invest_capital (
                date date PRIMARY KEY,
                "netProprietary" double precision,
                "netDomesticIndividual" double precision,
                "netDomesticInstitution" double precision,
                "netForeignIndividual" double precision,
                "netForeignInstitution" double precision
            )
        '''))

    # upsert n ngày mới nhất
    out = table.nlargest(n_days, 'date').copy()
    out['date'] = out['date'].dt.date
    cols = list(out.columns)
    rows = [tuple(None if pd.isna(x) else x for x in r)
            for r in out.itertuples(index=False, name=None)]

    col_sql    = ', '.join(f'"{c}"' for c in cols)
    update_sql = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'date')
    sql = f'''
        INSERT INTO exchange_history.invest_capital ({col_sql})
        VALUES %s
        ON CONFLICT (date) DO UPDATE SET {update_sql}
    '''

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()

    print(f"Đã upsert {len(rows)} dòng.")
    return len(rows)