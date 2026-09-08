import json
import requests
from kafka import KafkaProducer

# =====================================================
# Configuration
# =====================================================

import os
API_URL = os.environ.get("API_URL", "http://localhost:5000/api/sales")

import os
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")

TOPIC_NAME = "superstore_sales"

# =====================================================
# Create Kafka Producer
# =====================================================

try:

    producer = KafkaProducer(

        bootstrap_servers=KAFKA_SERVER,

        value_serializer=lambda v: json.dumps(v).encode("utf-8")

    )

    print("=" * 60)
    print("Kafka Producer Connected Successfully")
    print("=" * 60)

except Exception as e:

    print("Unable to Connect Kafka")
    print(e)
    exit(1)

# =====================================================
# Read data from API
# =====================================================

try:

    response = requests.get(API_URL)

    response.raise_for_status()

    sales_data = response.json()

    print(f"Fetched {len(sales_data)} records from API")

except Exception as e:

    print("API Extraction Failed")
    print(e)
    exit(1)

# =====================================================
# Incremental Load Tracking
# =====================================================
STATE_FILE = "last_processed_index.txt"

try:
    with open(STATE_FILE, "r") as f:
        last_index = int(f.read().strip())
except FileNotFoundError:
    last_index = 0

new_data = sales_data[last_index:]

if not new_data:
    print("\nNo new records to send to Kafka.")
    print("\n" + "=" * 60)
    print("Kafka Producer Completed Successfully")
    print("=" * 60)
    exit(0)

# =====================================================
# Send records to Kafka
# =====================================================

print(f"\nSending {len(new_data)} New Records to Kafka...\n")

count = 0

for record in new_data:

    producer.send(TOPIC_NAME, value=record)

    count += 1

    if count % 1000 == 0 or count == len(new_data):
        print(f"Record {count} Sent")

producer.flush()

# Update state file
with open(STATE_FILE, "w") as f:
    f.write(str(last_index + len(new_data)))

print("\n" + "=" * 60)
print(f"Total Records Sent : {count}")
print("Kafka Producer Completed Successfully")
print("=" * 60)