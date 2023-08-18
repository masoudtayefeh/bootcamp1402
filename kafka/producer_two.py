import pandas as pd
import json
from confluent_kafka import Producer
import json
from confluent_kafka.admin import AdminClient

kafka_server = {"bootstrap.servers": "127.0.0.1:9092"}
admin_client = AdminClient(kafka_server)
producer = Producer(kafka_server)


df_chunks = pd.read_csv('organizations-2000000.csv',chunksize=1000)
for idx, df in enumerate(df_chunks):
    for index, row in df.iterrows():
        record_value = row.to_dict()
        record_value_json = json.dumps(record_value, default=str)
        producer.produce(
            "pipeline_two_topic",
            key=str(row['Index']),
            value=record_value_json
        )

    producer.flush()
    print(f"1000 New messages inserted to kafka - df numeber: {idx}")