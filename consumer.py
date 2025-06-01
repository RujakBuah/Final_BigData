from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Kafka Configuration
KAFKA_TOPIC = "csv-topic"
KAFKA_BROKER = "localhost:9092"

# MongoDB settings 
MONGO_URL = "mongodb://localhost:27017"
MONGO_DB = "smoker_data"
MONGO_COLLECTION = "smokers"

# Connect to MongoDB
client = MongoClient(MONGO_URL)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="csv-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON
)

print("Consumer started. Waiting for messages...")

# Consume messages from Kafka and insert them into MongoDB
for message in consumer:
    data = message.value  # Extract the JSON message
    print(f"Received: {data}")

    # Insert data into MongoDB
    collection.insert_one(data)
    print("Inserted into MongoDB")