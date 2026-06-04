from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(
    lambda: {
        "count": 0,
        "revenue": 0.0,
        "min_amount": float('inf'),
        "max_amount": float('-inf')
    }
)

msg_count = 0

for message in consumer:
    transaction = message.value

    category = transaction["category"]
    amount = transaction["amount"]

    stats[category]["count"] += 1
    stats[category]["revenue"] += amount
    stats[category]["min_amount"] = min(stats[category]["min_amount"], amount)
    stats[category]["max_amount"] = max(stats[category]["max_amount"], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\nCategory      | Count | Revenue    | Min      | Max")
        print("-" * 60)

        for category, data in stats.items():
            print(
                f"{category:<13} | "
                f"{data['count']:>5} | "
                f"{data['revenue']:>10.2f} | "
                f"{data['min_amount']:>8.2f} | "
                f"{data['max_amount']:>8.2f}"
            )

        print("-" * 60)
