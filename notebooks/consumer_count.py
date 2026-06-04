from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

for message in consumer:
    transaction = message.value

    store = transaction["store"]
    amount = transaction["amount"]

    # 1. Increment store count
    store_counts[store] += 1

    # 2. Add amount to running total
    total_amount[store] = total_amount.get(store, 0) + amount

    msg_count += 1

    # 3. Print summary every 10 messages
    if msg_count % 10 == 0:
        print("\nStore | Count | Total Amount | Avg Amount")
        print("-" * 50)

        for store in store_counts:
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count

            print(
                f"{store:<10} | "
                f"{count:>5} | "
                f"{total:>12.2f} | "
                f"{avg:>10.2f}"
            )

        print("-" * 50)
