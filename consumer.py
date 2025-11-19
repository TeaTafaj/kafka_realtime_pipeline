import json
from confluent_kafka import Consumer
import psycopg2

# --- PostgreSQL connection ---
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="kafka_db",
    user="kafka_user",
    password="kafka_password",
)
cursor = conn.cursor()

# --- Kafka consumer config ---
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "trades-consumer-group-v2",  # new group id
    "auto.offset.reset": "latest",  # only new messages
}

consumer = Consumer(conf)
consumer.subscribe(["orders"])  # topic still named 'orders' for now

print("🟢 Consumer started. Waiting for trades...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue

        trade = json.loads(msg.value().decode("utf-8"))
        print("📥 Received raw message:", trade)

        # Ignore old 'order' messages that don't match our schema
        expected_keys = {"trade_id", "ticker", "price", "volume", "side", "timestamp"}
        if not expected_keys.issubset(trade.keys()):
            print("⚠️ Skipping non-trade message (old schema).")
            continue

        # Insert into Postgres
        cursor.execute(
            """
            INSERT INTO trades (trade_id, ticker, price, volume, side, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO NOTHING;
            """,
            (
                trade["trade_id"],
                trade["ticker"],
                trade["price"],
                trade["volume"],
                trade["side"],
                trade["timestamp"],
            ),
        )
        conn.commit()

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("🔴 Consumer stopped, DB connection closed.")
