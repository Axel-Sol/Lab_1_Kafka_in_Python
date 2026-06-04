from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='filter-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for large transactions (amount > 1000)...")

for message in consumer:
    transaction = message.value

    if transaction["amount"] > 5000:
        print(
            f"ALERT: {transaction['tx_id']} | "
            f"{transaction['amount']:.2f} PLN | "
            f"{transaction['store']} | "
            f"{transaction['category']}"
        )
