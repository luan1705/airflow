"""
Shared utilities for alert scripts:
- connect_kafka_poll: connect to kafka and poll messages once
- db helpers: ensure schema/tables, insert alert, upsert market data
- config: connection strings
"""

import json
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql
import pandas as pd

# === CONFIG ===
KAFKA_BOOTSTRAP = "broker.videv.cloud"
POSTGRES_DSN = "postgresql+psycopg2://vnsfintech:@Vns123456@videv.cloud:5432/vnsfintech"
PSYCOPG2_DSN = "dbname=vnsfintech user=vnsfintech password=@Vns123456 host=videv.cloud port=5432"
SCHEMA = "alert_function"

# === Kafka helper ===
def connect_kafka_poll(topics, timeout_ms=5000, max_records=10000, group_id="alert-group"):
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        consumer_timeout_ms=timeout_ms+1000
    )
    messages = []
    try:
        polled = consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        for tp, records in polled.items():
            for r in records:
                if r.value:
                    messages.append(r.value)
    finally:
        consumer.close()
    return messages

# === DB helpers ===
def get_conn():
    return psycopg2.connect(PSYCOPG2_DSN)

def ensure_schema_and_tables():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
    cur.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.alert_logs (
            id SERIAL PRIMARY KEY,
            alert_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            time TIMESTAMP NOT NULL,
            message TEXT,
            created_at TIMESTAMP DEFAULT now()
        )
    """).format(sql.Identifier(SCHEMA)))
    cur.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.market_data (
            symbol TEXT NOT NULL,
            time TIMESTAMP NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            PRIMARY KEY (symbol, time)
        )
    """).format(sql.Identifier(SCHEMA)))
    conn.commit()
    cur.close()
    conn.close()

def insert_alert(alert_type, symbol, time_ts, message):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql.SQL("""
        INSERT INTO {}.alert_logs (alert_type, symbol, time, message)
        VALUES (%s, %s, %s, %s)
    """).format(sql.Identifier(SCHEMA)), (alert_type, symbol, time_ts, message))
    conn.commit()
    cur.close()
    conn.close()

def bulk_upsert_market_data(records):
    if not records:
        return
    conn = get_conn()
    cur = conn.cursor()
    for r in records:
        cur.execute(sql.SQL("""
            INSERT INTO {}.market_data (symbol,time,open,high,low,close,volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, time) DO NOTHING
        """).format(sql.Identifier(SCHEMA)), (
            r.get("symbol"), r.get("time"), r.get("open"), r.get("high"),
            r.get("low"), r.get("close"), r.get("volume")
        ))
    conn.commit()
    cur.close()
    conn.close()

def load_recent_market_df(symbol, lookback=500):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql.SQL("""SELECT time, open, high, low, close, volume FROM {}.market_data
                          WHERE symbol=%s ORDER BY time DESC LIMIT %s""").format(sql.Identifier(SCHEMA)), (symbol, lookback))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        return pd.DataFrame(columns=["time","open","high","low","close","volume"])
    df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time').reset_index(drop=True)
    return df
