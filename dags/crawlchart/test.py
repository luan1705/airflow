import json
from kafka import KafkaConsumer
import psycopg2
from datetime import datetime

# =========================
# Config
# =========================
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "crypto_ohlc"

DB_URL = "postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech"

TOP10 = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
         "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"]

# =========================
# PostgreSQL Connection
# =========================
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

# Tạo schema + 10 bảng
cur.execute("CREATE SCHEMA IF NOT EXISTS other_indice;")
for sym in TOP10:
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS other_indice.{sym} (
        time TIMESTAMP PRIMARY KEY,
        interval TEXT NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume NUMERIC
    );
    """)
conn.commit()

# =========================
# Kafka Consumer
# =========================
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="crypto_group"
)

print("Consumer started...")

for msg in consumer:
    record = msg.value
    record["symbol"] = record["symbol"].upper()

    if record["symbol"] not in TOP10:
        continue

    # Nếu không có trường time thì gán bằng thời gian hiện tại
    if "time" in record:
        try:
            record["time"] = datetime.strptime(record["time"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            record["time"] = datetime.now()
    else:
        record["time"] = datetime.now()

    sql = f"""
    INSERT INTO other_indice.{record['symbol']}
    (time, interval, open, high, low, close, volume)
    VALUES (%(time)s, %(interval)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
    ON CONFLICT (time) DO UPDATE
    SET interval = EXCLUDED.interval,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;
    """

    try:
        cur.execute(sql, record)
        conn.commit()
        print(f"Upserted into {record['symbol']}:", record)
    except Exception as e:
        conn.rollback()
        print("DB error:", e)
