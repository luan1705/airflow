import config
import json
import time
import pandas as pd
from kafka import KafkaProducer
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
import os

# KAFKA_BROKER = 'localhost:9092'

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "streaming_stock")

# Tạo Kafka producer chung, threadsafe
producer = KafkaProducer(
    bootstrap_servers= KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_market_data(message):
    data = message.get('Content', '{}')
    data = json.loads(data)
    status = data.get('Side')
    status_map = {"BU": "Mua", "SD": "Bán", "unknown": "ATO/ATC"}
    status = status_map.get(status, status)
    
    time = pd.to_datetime(data['TradingDate'] + ' ' + data['Time'], format='%d/%m/%Y %H:%M:%S')
    time = time.strftime("%Y-%m-%d %H:%M:%S")

    result = {
        'time': time,
        'symbol': data['Symbol'],
        'open': data['PriorVal'] / 1000,
        'high': data['Highest'] / 1000,
        'low': data['Lowest'] / 1000,
        'close': data['LastPrice'] / 1000,
        'lastvolume': data['LastVol'],
        'status': status,
        'volume': data['TotalVol'],
        'exchange': data['Exchange']
    }

    # Gửi Kafka
    topic = data['Symbol']
    producer.send(topic, result)
    producer.flush()
    print(f"[{topic}] {result}")

def getError(error):
    print(error)

def getError(error):
    print(f"⚠️ WebSocket lỗi: {error}")

def start_stream():
    try:
        selected_channel = "X-Trade:ALL"
        print("🔌 Đang kết nối MarketDataStream...")
        mm = MarketDataStream(config, MarketDataClient(config))
        mm.start(get_market_data, getError, selected_channel)   
    except Exception as e:
        print(f"❌ Lỗi trong stream: {e}")

def main():
	start_stream()
	try:
		while True:
			time.sleep(1)
	except KeyboardInterrupt:
		print("🛑 Dừng streaimg.")

if __name__ == "__main__":
	main()

