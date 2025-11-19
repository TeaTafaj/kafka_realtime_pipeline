import json
import time
import random
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# Kafka configuration
conf = {"bootstrap.servers": "localhost:9092"}

producer = Producer(conf)

# ---- STOCK TRADES DOMAIN ----
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"]


def generate_trade():
    return {
        "trade_id": fake.uuid4(),
        "ticker": random.choice(TICKERS),
        "price": round(random.uniform(50, 500), 2),  # synthetic price
        "volume": random.randint(1, 1000),  # shares traded
        "side": random.choice(["buy", "sell"]),  # trade direction
        "timestamp": fake.iso8601(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Produced message to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


# main loop
while True:
    trade = generate_trade()
    producer.produce(
        topic="orders",  # we can rename the topic to "trades" later if you want
        value=json.dumps(trade).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(1)
    print("📤 Sent trade:", trade)
    time.sleep(1)
