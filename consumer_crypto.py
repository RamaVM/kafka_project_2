from kafka import KafkaConsumer
import json, pandas as pd, os, time

# Create Kafka Consumer
consumer = KafkaConsumer(
    'crypto_prices',  # topic name
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

file_path = 'crypto_prices.csv'
print(" Consumer started. Listening to topic 'crypto_prices'...")

while True:
    for message in consumer:
        data = message.value
        df = pd.DataFrame([data])
        df.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)
        print(f"[{time.strftime('%H:%M:%S')}] Stored → {data}")
