import re
import json
import requests
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Cấu hình ────────────────────────────────────────────────────────────────────
DB_URL = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
API_BASE = "https://iq.vietcap.com.vn/api/iq-insight-service/v1/news"

HEADERS = {
    "accept": "application/json",
    "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5",
    "origin": "https://trading.vietcap.com.vn",
    "referer": "https://trading.vietcap.com.vn/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
}

PDF_LIMIT = 50   # ← Số PDF cần cho mỗi symbol
MAX_WORKERS = 10

# ── DDL ─────────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS documents.link (
    id                VARCHAR(100) PRIMARY KEY,
    symbol            VARCHAR(20),
    news_source_link  TEXT,
    pdf_link          TEXT,
    upload_status     TEXT
);
"""

UPSERT_SQL = """
INSERT INTO documents.link (id, symbol, news_source_link, pdf_link)
VALUES (%(id)s, %(symbol)s, %(news_source_link)s, %(pdf_link)s)
ON CONFLICT (id) DO UPDATE SET
    symbol           = EXCLUDED.symbol,
    news_source_link = EXCLUDED.news_source_link,
    pdf_link         = EXCLUDED.pdf_link;
"""

PDF_PATTERN = re.compile(r'https?://[^\s"\'<>]+\.pdf', re.IGNORECASE)


# ── Helpers ──────────────────────────────────────────────────────────────────────
def extract_pdf_link(data: dict) -> str | None:
    for att in (data.get("newsAttachments") or []):
        url = att.get("attachmentLink") or att.get("url") or att.get("link") or ""
        if url.lower().endswith(".pdf"):
            return url

    source_link = data.get("newsSourceLink") or ""
    if source_link.lower().endswith(".pdf"):
        return source_link

    html_content = data.get("newsFullContent") or ""
    if html_content:
        matches = re.findall( r'(?:href|src)=["\']([^"\']+\.pdf)["\']', html_content, re.IGNORECASE)
        if matches:
            return matches[0]
        matches = PDF_PATTERN.findall(html_content)
        if matches:
            return matches[0]

    raw_str = json.dumps(data, ensure_ascii=False)
    matches = PDF_PATTERN.findall(raw_str)
    return matches[0] if matches else None


def fetch_detail(news_id: str) -> dict:
    resp = requests.get(f"{API_BASE}/{news_id}", headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", {})


def parse_row(news_id: str, symbol: str, data: dict) -> dict:
    return {
        "id":               news_id,
        "symbol":           data.get("ticker") or symbol,
        "news_source_link": data.get("newsSourceLink"),
        "pdf_link":         extract_pdf_link(data),
    }


def save_rows(rows: list, conn):
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=50)
    conn.commit()


# ── Query: lấy candidates theo symbol, mới nhất trước ───────────────────────────
def get_candidates_by_symbol(conn) -> dict[str, list[str]]:
    """
    Với mỗi symbol, lấy toàn bộ id từ documents.test chưa có pdf
    (chưa có trong link HOẶC pdf_link IS NULL), sắp xếp mới nhất trước.
    Trả về dict: { symbol: [id, ...] }
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.id, t.symbol
            FROM documents.test t
            LEFT JOIN documents.link l ON t.id = l.id
            WHERE (l.id IS NULL OR l.pdf_link IS NULL)
              AND l.upload_status IS NULL
            ORDER BY t.symbol, t.public_date DESC NULLS LAST;
        """)
        rows = cur.fetchall()

    by_symbol: dict[str, list[str]] = {}
    for news_id, symbol in rows:
        by_symbol.setdefault(symbol, []).append(news_id)

    total = sum(len(v) for v in by_symbol.values())
    print(f"-> {len(by_symbol)} symbols, {total} candidates chua co pdf\n")
    return by_symbol


# ── Xử lý 1 symbol ──────────────────────────────────────────────────────────────
def process_symbol(symbol: str, candidate_ids: list[str], conn, limit: int = PDF_LIMIT):
    """
    Fetch song song các id của symbol, dừng khi đủ `limit` pdf có link.
    """
    print(f"[{symbol}] Cần {limit} pdf | {len(candidate_ids)} candidates")

    pdf_rows = []      # rows có pdf_link
    no_pdf_rows = []   # rows không có pdf (vẫn lưu để đánh dấu đã check)
    checked = 0

    # Xử lý theo batch để dừng sớm
    batch_size = MAX_WORKERS * 2
    for batch_start in range(0, len(candidate_ids), batch_size):
        if len(pdf_rows) >= limit:
            break

        batch = candidate_ids[batch_start: batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_detail, nid): nid for nid in batch}

            for future in as_completed(futures):
                news_id = futures[future]
                checked += 1
                try:
                    data = future.result()
                    row = parse_row(news_id, symbol, data)

                    if row["pdf_link"]:
                        pdf_rows.append(row)
                        print(f"  [{symbol}] PDF {len(pdf_rows)}/{limit} | {news_id}")
                    else:
                        no_pdf_rows.append(row)

                except Exception as e:
                    print(f"  [{symbol}] Lỗi {news_id}: {e}")

        # Lưu batch vào DB ngay để không mất dữ liệu nếu crash
        save_rows(pdf_rows + no_pdf_rows, conn)
        pdf_rows_saved = len(pdf_rows)
        no_pdf_rows = []  # reset, đã lưu

        if pdf_rows_saved >= limit:
            break

    print(f"  ✓ [{symbol}] Xong: {len(pdf_rows)} pdf sau {checked} lần fetch\n")
    return len(pdf_rows)


# ── Already-have check: bỏ qua symbol đã đủ pdf ─────────────────────────────────
def get_pdf_counts(conn) -> dict[str, int]:
    """Đếm số pdf_link đã có cho từng symbol trong documents.link."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, COUNT(*) 
            FROM documents.link
            WHERE pdf_link IS NOT NULL
            GROUP BY symbol;
        """)
        return {row[0]: row[1] for row in cur.fetchall()}


# ── Main ─────────────────────────────────────────────────────────────────────────
def main():
    print(f"Kết nối DB: {DB_URL}")
    conn = psycopg2.connect(DB_URL)
    print("✓ Kết nối thành công.\n")

    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    # Kiểm tra symbol nào đã đủ pdf rồi
    existing_counts = get_pdf_counts(conn)
    for sym, cnt in existing_counts.items():
        status = "✓ đủ" if cnt >= PDF_LIMIT else f"còn thiếu {PDF_LIMIT - cnt}"
        print(f"  {sym}: {cnt} pdf ({status})")
    print()

    # Lấy candidates (chưa có pdf), nhóm theo symbol
    candidates_by_symbol = get_candidates_by_symbol(conn)

    total_symbols = len(candidates_by_symbol)
    summary = {}

    for idx, (symbol, candidate_ids) in enumerate(candidates_by_symbol.items(), 1):
        already = existing_counts.get(symbol, 0)
        still_need = PDF_LIMIT - already

        if still_need <= 0:
            print(f"[{idx}/{total_symbols}] {symbol}: đã đủ {PDF_LIMIT} pdf, bỏ qua.")
            continue

        print(f"[{idx}/{total_symbols}] {symbol}: đã có {already}, cần thêm {still_need} pdf")
        found = process_symbol(symbol, candidate_ids, conn, limit=still_need)
        summary[symbol] = already + found

    conn.close()

    print("\n── Tổng kết ────────────────────────────────")
    for sym, total in summary.items():
        flag = "✓" if total >= PDF_LIMIT else f"⚠ chỉ {total}/{PDF_LIMIT}"
        print(f"  {sym}: {total} pdf {flag}")
    print("✓ Hoàn tất.")


if __name__ == "__main__":
    main()