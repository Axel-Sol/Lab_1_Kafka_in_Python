from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    return {
        "tx_id": f"TX{random.randint(1, 9999):04d}",
        "user_id": f"u{random.randint(1, 20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(["Warsaw", "Krakow", "Gdansk", "Wroclaw"]),
        "category": random.choice(["electronics", "clothing", "food", "books"]),
        "timestamp": datetime.now().isoformat()
    }

for _ in range(50):
    transaction = generate_transaction()

    producer.send('transactions', value=transaction)
    producer.flush()

    print(transaction)

    time.sleep(1)

producer.close()
