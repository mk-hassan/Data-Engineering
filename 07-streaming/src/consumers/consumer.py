import json

from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-rides-consumer',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=10_000  # stop after 10s of no new messages
)

print(f"Reading all messages from '{topic_name}'...")

total = 0
long_trips = 0

for message in consumer:
    ride = message.value
    total += 1
    if ride['trip_distance'] > 5.0:
        long_trips += 1

consumer.close()

print(f"Total trips:            {total}")
print(f"Trips with distance > 5: {long_trips}")
