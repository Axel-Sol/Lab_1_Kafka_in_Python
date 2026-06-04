from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='velocity-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(list)

for message in consumer:
    transaction = message.value

    user_id = transaction["user_id"]
    tx_time = datetime.fromisoformat(transaction["timestamp"])

    # Add current transaction time
    user_transactions[user_id].append(tx_time)

    # Keep only transactions from the last 60 seconds
    cutoff = tx_time - timedelta(seconds=60)

    user_transactions[user_id] = [
        t for t in user_transactions[user_id]
        if t >= cutoff
    ]

    # Alert if more than 3 transactions within 60 seconds
    if len(user_transactions[user_id]) > 3:
        print(
            f"VELOCITY ALERT: {user_id} made "
            f"{len(user_transactions[user_id])} transactions "
            f"within 60 seconds"
        )
