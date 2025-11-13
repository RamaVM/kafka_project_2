from kafka import KafkaProducer
import json, time, requests

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

# Small helper to print with timestamp
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

while True:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "bitcoin" in data and "usd" in data["bitcoin"]:
            price = data["bitcoin"]["usd"]
            message = {
                'currency': 'BTC',
                'price_usd': price,
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            log(f"Producing: {message}")
            producer.send('crypto_prices', message)
            time.sleep(10)
        else:
            log(f"⚠️ API responded unexpectedly: {data}")
            time.sleep(30)

    except requests.exceptions.Timeout:
        log("⏳ Timeout — API took too long to respond. Retrying in 20s...")
        time.sleep(20)

    except requests.exceptions.ConnectionError:
        log("🌐 Network glitch or rate-limit — waiting 20s before retry...")
        time.sleep(20)

    except Exception as e:
        log(f"❌ Unexpected error: {e}")
        time.sleep(20)
