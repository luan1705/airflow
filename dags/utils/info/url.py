import re
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import concurrent.futures

headers = {
  'Cookie': '__Secure-better-auth.session_token=67ahD0YRN0VvUfQFJTZ66G2dU99QkO8S.0Mrc0z5D1n7DP%2Bjmv1bSTqIcKwi3ZteatV94FjLWTOo%3D; __Secure-better-auth.session_data=eyJzZXNzaW9uIjp7InNlc3Npb24iOnsiZXhwaXJlc0F0IjoiMjAyNi0wNi0xNVQwNzoyODowNS41NDZaIiwidG9rZW4iOiI2N2FoRDBZUk4wVnZVZlFGSlRaNjZHMmRVOTlRa084UyIsImNyZWF0ZWRBdCI6IjIwMjYtMDYtMDhUMDc6Mjg6MDUuNTQ2WiIsInVwZGF0ZWRBdCI6IjIwMjYtMDYtMDhUMDc6Mjg6MDUuNTQ2WiIsImlwQWRkcmVzcyI6IjEwLjQyLjAuMTQ3IiwidXNlckFnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzE0OC4wLjAuMCBTYWZhcmkvNTM3LjM2IiwidXNlcklkIjoiaUV5YTZXNEFQUzAyUWFibHRtR2o2M3JMWTRFeHJyZUIiLCJpbXBlcnNvbmF0ZWRCeSI6bnVsbCwiYWN0aXZlT3JnYW5pemF0aW9uSWQiOm51bGwsImFjdGl2ZVRlYW1JZCI6bnVsbCwiaWQiOiI5YWlWaHVvUjRBYnIwN0lURUIxSVcyR1dwV09qdzNBZCJ9LCJ1c2VyIjp7Im5hbWUiOiJQaGFuIExvaSIsImVtYWlsIjoiY3NvbGl1bG9AZ21haWwuY29tIiwiZW1haWxWZXJpZmllZCI6dHJ1ZSwiaW1hZ2UiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQ2c4b2NMaGFoNkYwa0JLckh1RmZsdG0wdnpKamZKWVJ1VFo2RWVYZ1VXSWdqWWlseDk1RGc9czk2LWMiLCJjcmVhdGVkQXQiOiIyMDI2LTA1LTI1VDAyOjU2OjU0LjM4N1oiLCJ1cGRhdGVkQXQiOiIyMDI2LTA1LTI1VDAyOjU2OjU0LjM4N1oiLCJ1c2VybmFtZSI6bnVsbCwiZGlzcGxheVVzZXJuYW1lIjpudWxsLCJyb2xlIjoidXNlciIsImJhbm5lZCI6ZmFsc2UsImJhblJlYXNvbiI6bnVsbCwiYmFuRXhwaXJlcyI6bnVsbCwidXNlclR5cGUiOm51bGwsImRpc3BsYXlQaG9uZU51bWJlciI6bnVsbCwiaWQiOiJpRXlhNlc0QVBTMDJRYWJsdG1HajYzckxZNEV4cnJlQiJ9LCJ1cGRhdGVkQXQiOjE3ODA5MDM2ODU1NzQsInZlcnNpb24iOiIxIn0sImV4cGlyZXNBdCI6MTc4MDkwMzk4NTU3NCwic2lnbmF0dXJlIjoiTC04Q0o4MDA4ZXpYMnFoY0JuZlFGcU9mUlJKWHMxRDdWOWI5ZHZQbEE3YyJ9; ph_phc_2O2eCgo6AOpwUykoQ5ufJGvaahcsg9cOPCMp4sZwSMh_posthog=%7B%22%24device_id%22%3A%220198d510-c557-74c1-98a3-4deb13835f48%22%2C%22distinct_id%22%3A%220198d510-c557-74c1-98a3-4deb13835f48%22%2C%22%24sesid%22%3A%5B1780903985308%2C%22019ea60a-2ff9-7412-a60f-d749c2504c20%22%2C1780902146032%5D%2C%22%24initial_person_info%22%3A%7B%22r%22%3A%22%24direct%22%2C%22u%22%3A%22https%3A%2F%2Fsstock.vn%2Ftong-quan-kenh-tai-san%22%7D%2C%22%24user_state%22%3A%22anonymous%22%7D'
}

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech",
    poolclass= NullPool
)

def normalize_url(raw_url):
    if not raw_url or not str(raw_url).strip():
        return None
    u = str(raw_url).strip()
    # Bỏ path, chỉ lấy scheme + domain
    if '://' in u:
        parts = u.split('://')
        scheme = parts[0]
        domain = parts[1].split('/')[0]
        return f"{scheme}://{domain}"
    else:
        domain = u.split('/')[0]
        return f"https://{domain}"

def fetch_and_upsert(symbol):
    try:
        url = f"https://api-feature.sstock.vn/api/v1/company/info/{symbol}"
        response = requests.get(url, headers=headers).json()
        if not response.get('data'):
            return
        data = response['data']
        df = pd.json_normalize(data)[['code', 'url']]
        df.columns = ['symbol', 'url']

        with engine.begin() as conn:
            for _, row in df.iterrows():
                clean_url = normalize_url(row['url'])
                conn.execute(text("""
                    INSERT INTO info.asset (symbol, url)
                    VALUES (:symbol, :url)
                    ON CONFLICT (symbol) DO UPDATE SET
                        url = EXCLUDED.url
                """), {'symbol': row['symbol'], 'url': clean_url})

        print(f"✅ {symbol}")
    except Exception as e:
        print(f"❌ {symbol}: {e}")

def url():
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE info.asset ADD COLUMN IF NOT EXISTS url TEXT
        """))

    symbols = pd.read_sql(text("""
        SELECT symbol FROM info.asset
        WHERE type = 'Stock'
          AND exchange IN ('HOSE', 'HNX', 'UPCOM')
    """), engine)['symbol'].tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_and_upsert, symbols)

    print("Hoàn tất!")