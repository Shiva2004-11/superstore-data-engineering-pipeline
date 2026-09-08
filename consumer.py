from kafka import KafkaConsumer
import json
import pandas as pd

TOPIC = "superstore_sales"
import os
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_SERVER", "localhost:9092")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="superstore_etl_group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=5000
)

print("=" * 60)
print("Kafka Consumer Connected Successfully")
print("=" * 60)

records = []

print("\nReceiving Records...\n")

for i, message in enumerate(consumer):

    data = message.value
    records.append(data)

    print(f"Record {i+1} Received")

consumer.close()

print("\nSaving records to CSV...")

df = pd.DataFrame(records)

df.to_csv("processed_superstore.csv", index=False)

print("=" * 60)
print(f"Total Records Received : {len(records)}")
print("processed_superstore.csv Created Successfully")
print("=" * 60)