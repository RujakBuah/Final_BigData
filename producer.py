from kafka import KafkaProducer
import pandas as pd
import json
import time

# Kafka Configuration
KAFKA_TOPIC = "csv-topic"
KAFKA_BROKER = "localhost:9092"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize as JSON
)

# Read CSV File
csv_file = "/mnt/c/Users/User/OneDrive/Documents/wtf_is_this_shit/final-big_anal/smoking_data.csv"
df = pd.read_csv(csv_file)

print("Starting continuous message sending... (Press Ctrl+C to stop)")

try:
    while True:
        for _, row in df.iterrows():
            data = row.to_dict()
            producer.send(KAFKA_TOPIC, value=data)
            print(f"Sent: {data}")
            time.sleep(2)  # wait 2 seconds between messages
        # Optionally, you can shuffle or modify data here before looping again
except KeyboardInterrupt:
    print("\nStopped by user.")

producer.flush()
producer.close()
print("Producer closed.")
