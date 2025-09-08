import os
import json
import time
import threading
import websocket
from kafka import KafkaProducer

# ================== Config ==================
API_KEY   = os.getenv("API_KEY", "NmIOKz0y94pojwSg9aZQBSTAzeC3Wcmt")
WS_URL    = "wss://socket.polygon.io/indices"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "world_stock_index")

# ================== Kafka Producer ==================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ================== WebSocket Callbacks ==================
def process_message(event):
    """ Xử lý dữ liệu và gửi vào Kafka """
    try:
        # Gửi thẳng toàn bộ event
        producer.send(KAFKA_TOPIC, event)
        producer.flush()
        print(f"[{KAFKA_TOPIC}] {event}")
    except Exception as e:
        print(f"❌ Lỗi gửi Kafka: {e}")

def on_open(ws):
    print("🔌 WebSocket opened, authenticating...")
    ws.send(json.dumps({"action": "auth", "params": API_KEY}))
    ws.send(json.dumps({"action": "subscribe", "params": "A.I:DJI,AM.I:DJI"}))  # ví dụ: Dow Jones

def on_message(ws, message):
    try:
        data = json.loads(message)
        if isinstance(data, list):
            for event in data:
                process_message(event)
        else:
            process_message(data)
    except Exception as e:
        print(f"⚠️ Error processing message: {e}")

def on_error(ws, err):
    print(f"⚠️ WebSocket error: {err}")

def on_close(ws, *_):
    print("🛑 WebSocket closed")

# ================== Stream Starter ==================
def start_stream():
    try:
        print("🚀 Connecting to Polygon WebSocket...")
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
    except Exception as e:
        print(f"❌ Lỗi khi khởi động WebSocket: {e}")

# ================== Main ==================
def main():
    t = threading.Thread(target=start_stream, daemon=True)
    t.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Dừng streaming.")

if __name__ == "__main__":
    main()
