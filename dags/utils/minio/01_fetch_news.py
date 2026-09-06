import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.create_list.symbol_list import (
    HOSE, HNX, UPCOM
)

# ── Cấu hình ────────────────────────────────────────────────────────────────────
DB_URL = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
API_URL = "https://iq.vietcap.com.vn/api/iq-insight-service/v1/news-events-for-chart"

HEADERS = {
    "accept": "application/json",
    "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5",
    "origin": "https://trading.vietcap.com.vn",
    "referer": "https://trading.vietcap.com.vn/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

MAX_WORKERS = 10
LIMIT = 100  # ← Số tin tối đa muốn lấy

# ── DDL ─────────────────────────────────────────────────────────────────────────
CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS documents;"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS documents.test (
    id                  VARCHAR(100) PRIMARY KEY,
    symbol              VARCHAR(20)  NOT NULL,
    news_title          TEXT,
    news_short_content  TEXT,
    public_date         TIMESTAMP
);
"""

UPSERT_SQL = """
INSERT INTO documents.test (id, symbol, news_title, news_short_content, public_date)
VALUES (%(id)s, %(symbol)s, %(news_title)s, %(news_short_content)s, %(public_date)s)
ON CONFLICT (id) DO UPDATE SET
    symbol             = EXCLUDED.symbol,
    news_title         = EXCLUDED.news_title,
    news_short_content = EXCLUDED.news_short_content,
    public_date        = EXCLUDED.public_date;
"""


def date_chunks_recent(chunk_months: int = 3):
    """Tạo chunks từ hiện tại đi ngược về quá khứ (mới nhất trước)."""
    today = datetime.today()
    chunks = []
    cursor = today
    while cursor.year >= 2016:
        start = cursor - relativedelta(months=chunk_months) + timedelta(days=1)
        chunks.append((start.strftime("%Y%m%d"), cursor.strftime("%Y%m%d")))
        cursor = start - timedelta(days=1)
    return chunks  # Thứ tự: mới nhất → cũ nhất


def fetch_chunk(ticker: str, from_date: str, to_date: str,
                language_id: int = 1, event_code: str = "DIV,ISS") -> tuple:
    params = {
        "ticker":     ticker.upper(),
        "fromDate":   from_date,
        "toDate":     to_date,
        "languageId": language_id,
        "eventCode":  event_code,
    }
    resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    return from_date, to_date, (data if isinstance(data, list) else [])


def parse_row(ticker: str, item: dict) -> dict:
    public_date = None
    raw_date = item.get("publicDate")
    if raw_date:
        try:
            public_date = datetime.strptime(raw_date[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    return {
        "id":                 str(item.get("id")),
        "symbol":             ticker.upper(),
        "news_title":         item.get("newsTitle"),
        "news_short_content": item.get("newsShortContent"),
        "public_date":        public_date,
    }


def save_to_db(rows: list, conn) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


def process_ticker(ticker: str, conn, limit: int = LIMIT, chunk_months: int = 3):
    """
    Fetch từng chunk từ mới → cũ, dừng lại khi đủ `limit` tin.
    Dùng ThreadPoolExecutor theo batch nhỏ để kiểm soát việc dừng sớm.
    """
    chunks = date_chunks_recent(chunk_months)
    total_chunks = len(chunks)
    print(f"\n[{ticker.upper()}] Tìm {limit} tin mới nhất ({total_chunks} chunks có thể)...")

    all_rows = []
    chunks_done = 0

    # Xử lý theo batch nhỏ để có thể dừng sớm khi đủ dữ liệu
    batch_size = MAX_WORKERS
    for batch_start in range(0, total_chunks, batch_size):
        if len(all_rows) >= limit:
            break  # ← Đã đủ tin, không fetch thêm

        batch = chunks[batch_start: batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_chunk, ticker, f, t): (f, t)
                for f, t in batch
            }
            for future in as_completed(futures):
                chunks_done += 1
                from_date, to_date = futures[future]
                try:
                    _, _, items = future.result()
                    if items:
                        rows = [parse_row(ticker, item) for item in items]
                        all_rows.extend(rows)
                        print(f"  [{chunks_done}] {from_date}-{to_date}: {len(rows)} tin "
                              f"(tổng: {len(all_rows)})")
                    else:
                        print(f"  [{chunks_done}] {from_date}-{to_date}: trống")
                except Exception as e:
                    print(f"  [{chunks_done}] {from_date}-{to_date}: Lỗi {e}")

    # Sắp xếp theo ngày mới nhất, cắt đúng limit
    all_rows.sort(key=lambda r: r["public_date"] or datetime.min, reverse=True)
    final_rows = all_rows[:limit]

    saved = save_to_db(final_rows, conn)
    print(f"  ✓ Đã lưu {saved} tin mới nhất cho {ticker.upper()}")


def main():
    symbols = HOSE + HNX + UPCOM # ← Thêm mã cổ phiếu vào đây

    tickers = list(dict.fromkeys(t.strip().upper() for t in symbols if t.strip()))

    print(f"Kết nối DB: {DB_URL}")
    conn = psycopg2.connect(DB_URL)
    print("✓ Kết nối thành công.\n")

    with conn.cursor() as cur:
        cur.execute(CREATE_SCHEMA_SQL)
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("✓ Bảng documents.test đã sẵn sàng.")

    for ticker in tickers:
        process_ticker(ticker, conn, limit=LIMIT)

    conn.close()
    print("\n✓ Hoàn tất.")


if __name__ == "__main__":
    main()