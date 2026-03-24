import json
from time import time

import pandas as pd
from kafka import KafkaProducer


topic_name = 'green-trips'
server = "localhost:9092"
data_path = 'data/green_tripdata_2025-10.parquet'

columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount',
]

df = pd.read_parquet(data_path, columns=columns)

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

t0 = time()

for _, row in df.iterrows():
    record = row.to_dict()
    record['lpep_pickup_datetime'] = str(record['lpep_pickup_datetime'])
    record['lpep_dropoff_datetime'] = str(record['lpep_dropoff_datetime'])
    producer.send(topic_name, value=record)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')