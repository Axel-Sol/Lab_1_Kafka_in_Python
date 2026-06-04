from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrich-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    transaction = message.value

    amount = transaction["amount"]

    if amount > 3000:
        transaction["risk_level"] = "HIGH"
    elif amount > 1000:
        transaction["risk_level"] = "MEDIUM"
    else:
        transaction["risk_level"] = "LOW"

    print(transaction)
