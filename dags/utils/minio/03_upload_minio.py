"""
Tối ưu hóa:
- ThreadPoolExecutor với 20 workers (tăng từ 5)
- Buffer download 4MB thay vì stream thô
- DB batch 100, dùng connection pool psycopg2
- Tách luồng download và DB update riêng biệt (queue)
- Retry tự động 3 lần khi lỗi mạng
- Đo thời gian từng bước

pip install requests psycopg2-binary minio urllib3
"""

import html
import io
import time
import queue
import threading
import urllib3
import requests
import psycopg2
import psycopg2.extras
import psycopg2.pool
from minio import Minio
from minio.error import S3Error
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Config ────────────────────────────────────────────────────────────────────
DB_URL           = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
MINIO_ENDPOINT   = "minio-server:9000"
MINIO_ACCESS_KEY = "tsjjRn5kFRhHDfYHkWe3"
MINIO_SECRET_KEY = "HNFt1AZU1tDrki5YbDwcsuZSt0AROzIZa2A1Xgib"
MINIO_SECURE     = False
BUCKET_NAME      = "pdf"

MAX_WORKERS      = 20           # ✅ tăng từ 5 → 20 luồng song song
DB_BATCH_SIZE    = 100          # batch update DB
UPLOAD_BATCH_SIZE = 500         # chỉ giữ tối đa 500 record/future trong RAM mỗi vòng
REQUEST_TIMEOUT  = 30
MINIO_PART_SIZE  = 32 * 1024 * 1024   # 32MB
DOWNLOAD_BUFFER  = 4 * 1024 * 1024    # 4MB buffer khi đọc PDF
MAX_RETRY        = 3                   # ✅ retry tự động

HNX_BASE_URL = "https://hnx.vn/"

DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,application/octet-stream,*/*",
}

_thread_local = threading.local()

def get_worker_session():
    """Mỗi worker thread giữ một requests.Session riêng."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(DOWNLOAD_HEADERS)
        s.verify = False
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS,
            max_retries=0,
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _thread_local.session = s
    return _thread_local.session

ALTER_TABLE_SQL = """
ALTER TABLE documents.link
    ADD COLUMN IF NOT EXISTS minio_path    TEXT,
    ADD COLUMN IF NOT EXISTS upload_status VARCHAR(20) DEFAULT 'pending';
"""

UPDATE_STATUS_SQL = """
UPDATE documents.link
SET minio_path    = %(minio_path)s,
    upload_status = %(status)s
WHERE id = %(id)s;
"""

# ── Helpers ───────────────────────────────────────────────────────────────────
def resolve_url(pdf_url: str) -> str:
    pdf_url = html.unescape(pdf_url)
    return pdf_url if pdf_url.startswith("http") else HNX_BASE_URL + pdf_url

def get_filename(pdf_url: str) -> str:
    pdf_url = html.unescape(pdf_url)
    if "_dl_name=" in pdf_url:
        for part in pdf_url.split("&"):
            if "_dl_name=" in part:
                return part.split("=", 1)[1]
    return pdf_url.rstrip("/").split("/")[-1].split("?")[0]

def get_pdf_records(pool, last_id=None, limit=UPLOAD_BATCH_SIZE):
    """Lấy từng batch PDF cần upload, keyset theo id để không giữ toàn bộ 80k+ rows trong RAM."""
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if last_id is None:
                cur.execute("""
                    SELECT id, symbol, pdf_link
                    FROM documents.link
                    WHERE pdf_link IS NOT NULL
                      AND (upload_status IS NULL OR upload_status != 'done')
                    ORDER BY id
                    LIMIT %s;
                """, (limit,))
            else:
                cur.execute("""
                    SELECT id, symbol, pdf_link
                    FROM documents.link
                    WHERE pdf_link IS NOT NULL
                      AND (upload_status IS NULL OR upload_status != 'done')
                      AND id > %s
                    ORDER BY id
                    LIMIT %s;
                """, (last_id, limit))
            return cur.fetchall()
    finally:
        pool.putconn(conn)

def ensure_bucket(client: Minio):
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"→ Đã tạo bucket: {BUCKET_NAME}")

# ── DB Writer Thread ──────────────────────────────────────────────────────────
# Chạy riêng 1 thread chuyên flush DB để không block worker threads
def db_writer(result_queue: queue.Queue, pool: psycopg2.pool.ThreadedConnectionPool, total: int):
    """
    Thread riêng chuyên nhận kết quả từ queue và batch-update DB.
    Không block các worker thread download/upload.
    """
    buffer   = []
    success  = 0
    failed   = 0
    done     = 0

    def flush(buf):
        if not buf:
            return
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur, UPDATE_STATUS_SQL, buf, page_size=DB_BATCH_SIZE
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"⚠️  DB flush error: {e}")
        finally:
            pool.putconn(conn)

    while True:
        try:
            item = result_queue.get(timeout=2)
        except queue.Empty:
            # Flush nốt những gì còn lại rồi thoát nếu đã xử lý hết
            if done >= total:
                flush(buffer)
                break
            continue

        if item is None:  # sentinel
            flush(buffer)
            break

        result, rec = item
        buffer.append(result)
        done += 1

        icon  = "✓" if result["status"] == "done" else "✗"
        fname = get_filename(rec["pdf_link"])
        print(f"[{done}/{total}] {icon} {rec['symbol'].upper()}/{fname}")

        if result["status"] == "done":
            success += 1
        else:
            failed += 1
            print(f"         ↳ Lỗi: {result['msg']}")

        # ✅ Flush mỗi 100 records
        if len(buffer) >= DB_BATCH_SIZE:
            flush(buffer)
            buffer.clear()

    print(f"\n✅ Hoàn tất: {success} thành công, {failed} lỗi / {total} tổng")

# ── Worker ────────────────────────────────────────────────────────────────────
def download_and_upload(record, minio_client: Minio):
    """
    Download PDF vào RAM buffer, upload lên MinIO.
    Retry tối đa MAX_RETRY lần.
    """
    session     = get_worker_session()
    news_id     = record["id"]
    symbol      = record["symbol"].upper()
    pdf_url     = resolve_url(record["pdf_link"])
    filename    = get_filename(record["pdf_link"])
    object_name = f"{symbol}/{filename}"

    for attempt in range(1, MAX_RETRY + 1):
        try:
            # ✅ Download vào RAM buffer (nhanh hơn stream thô)
            with session.get(pdf_url, timeout=REQUEST_TIMEOUT, stream=True) as resp:
                resp.raise_for_status()
                buf = io.BytesIO()
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_BUFFER):
                    if chunk:
                        buf.write(chunk)
                file_size = buf.tell()
                buf.seek(0)

            # ✅ Upload với length chính xác → MinIO không cần multipart guess
            minio_client.put_object(
                bucket_name  = BUCKET_NAME,
                object_name  = object_name,
                data         = buf,
                length       = file_size,
                part_size    = MINIO_PART_SIZE,
                content_type = "application/pdf",
            )

            return {
                "id":         news_id,
                "minio_path": f"{BUCKET_NAME}/{object_name}",
                "status":     "done",
                "msg":        "OK",
            }

        except (requests.RequestException, S3Error, Exception) as e:
            if attempt < MAX_RETRY:
                time.sleep(1.5 * attempt)   # backoff: 1.5s, 3s
                continue
            return {
                "id":         news_id,
                "minio_path": None,
                "status":     "error",
                "msg":        str(e),
            }

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    t_start = time.time()

    # ✅ Connection pool: tối đa MAX_WORKERS + 2 connections
    print("→ Kết nối DB pool...")
    pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=MAX_WORKERS + 2,
        dsn=DB_URL,
    )

    init_conn = pool.getconn()
    with init_conn.cursor() as cur:
        cur.execute(ALTER_TABLE_SQL)
    init_conn.commit()
    pool.putconn(init_conn)

    print(f"→ Kết nối MinIO: {MINIO_ENDPOINT}")
    http_client = urllib3.PoolManager(
        num_pools=max(10, MAX_WORKERS),
        maxsize=max(12, MAX_WORKERS + 4),
        timeout=urllib3.Timeout(connect=10.0, read=600.0),
        retries=urllib3.Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1,
        ),
    )

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
        http_client=http_client,
    )
    ensure_bucket(minio_client)

    print(
        f"→ Chạy {MAX_WORKERS} luồng song song | "
        f"upload batch={UPLOAD_BATCH_SIZE} | DB batch={DB_BATCH_SIZE}\n"
    )

    total_done = 0
    batch_no = 0
    last_id = None

    while True:
        records = get_pdf_records(pool, last_id=last_id, limit=UPLOAD_BATCH_SIZE)
        if not records:
            break

        batch_no += 1
        batch_total = len(records)
        last_id = records[-1]["id"]

        print(f"\n── Batch {batch_no}: {batch_total} PDF | last_id={last_id} ──")

        result_queue = queue.Queue(maxsize=MAX_WORKERS * 4)
        writer_thread = threading.Thread(
            target=db_writer,
            args=(result_queue, pool, batch_total),
            daemon=False,
        )
        writer_thread.start()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(download_and_upload, rec, minio_client): rec
                for rec in records
            }
            for future in as_completed(futures):
                rec = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {
                        "id": rec["id"],
                        "minio_path": None,
                        "status": "error",
                        "msg": str(e),
                    }
                result_queue.put((result, rec))

        result_queue.put(None)
        writer_thread.join()

        total_done += batch_total
        print(f"→ Đã xử lý trong phiên này: {total_done} file")

        del futures
        del records

    pool.closeall()

    elapsed = time.time() - t_start
    speed = total_done / elapsed if elapsed > 0 else 0
    print(f"\n✅ Kết thúc: {total_done} file trong phiên này")
    print(f"⏱  Tổng thời gian: {elapsed:.1f}s | Tốc độ: {speed:.1f} file/s")
    print("🔗 Xem file: https://s3-console.tanhungsoft.com")


if __name__ == "__main__":
    main()