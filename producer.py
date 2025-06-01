from kafka import KafkaProducer
import pandas as pd
import json

# Kafka Configuration
KAFKA_TOPIC = "csv-topic"
KAFKA_BROKER = "localhost:9092"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize as JSON
)

# Read CSV File
csv_file = "/mnt/c/Users/User/OneDrive/Documents/wtf_is_this_shit/final-big_anal/smoking_data.csv" # Change this to your CSV file path
df = pd.read_csv(csv_file)

# Send each row as a Kafka message
for _, row in df.iterrows():
    data = row.to_dict()  # Convert row to dictionary
    producer.send(KAFKA_TOPIC, value=data)
    print(f"Sent: {data}")

producer.flush()
producer.close()
print("Producer finished sending messages.")