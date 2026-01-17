import config
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
import os
import traceback

# ================== Config Kafka ==================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "172.20.0.3:9092")
TOPIC_COMMON = os.getenv("KAFKA_TOPIC_COMMON", "history_orderMatching_index")  # topic chung

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ================== Xử lý dữ liệu ==================
def handle_message(message):
    try:
        content_str = message.get("Content", "{}")
        content = json.loads(content_str)
        symbol = content.get("Symbol", "UNKNOWN")

        trading_date = content.get("TradingDate", "")
        time_str = content.get("Time", "")
        try:
            combined_time = datetime.strptime(
                f"{trading_date} {time_str}", "%d/%m/%Y %H:%M:%S"
            ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            combined_time = f"{trading_date} {time_str}"

        filtered_content = {
            "Time": combined_time,
            "Symbol": symbol,
            "Ceiling": content.get("Ceiling"),
            "RefPrice": content.get("RefPrice"),
            "High": content.get("Highest"),
            "Low": content.get("Lowest"),
            "AvgPrice": content.get("AvgPrice"),
            "LastPrice": content.get("LastPrice"),
            "LastVol": content.get("LastVol"),
            "Floor": content.get("Floor"),
            "PriorVal": content.get("PriorVal"),
            "Change": content.get("Change"),
            "RatioChange": content.get("RatioChange"),
            "EstMatchedPrice": content.get("EstMatchedPrice"),
            "TotalVol": content.get("TotalVol"),
            "TotalVal": content.get("TotalVal"),
            "Exchange": content.get("Exchange"),
            "TradingSession": content.get("TradingSession"),
            "Side": content.get("Side"),
        }

        producer.send(TOPIC_COMMON, filtered_content)
        topic_symbol = f"ssi.XTRADE.{symbol}"
        producer.send(topic_symbol, filtered_content)
        producer.flush()

        print(f"[{symbol}] gửi lên topic '{TOPIC_COMMON}' và '{topic_symbol}'")
    except Exception as e:
        print(f"⚠️ Lỗi xử lý message: {e}")
        traceback.print_exc()


def handle_error(error):
    print(f"⚠️ WebSocket lỗi: {error}")


# ================== Start Streaming ==================
def start_stream():
    selected_channel = "X-TRADE:ALL"
    while True:
        try:
            print("🔌 Kết nối MarketDataStream SSI...")
            stream = MarketDataStream(config, MarketDataClient(config))
            print("⏳ Đợi dữ liệu từ SSI...")
            # thêm ping_interval để giữ kết nối sống
            stream.start(handle_message, handle_error, selected_channel, ping_interval=20)
        except Exception as e:
            print(f"⚠️ Stream bị lỗi: {e}")
            traceback.print_exc()
            print("🔁 Thử reconnect sau 0.5s...")
            time.sleep(0.5)


# ================== Main ==================
def main():
    try:
        start_stream()
    except KeyboardInterrupt:
        print("🛑 Dừng streaming SSI.")


if __name__ == "__main__":
    main()
