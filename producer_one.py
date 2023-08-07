import random
from typing import List, Dict
from confluent_kafka import Producer
import json
from randomtimestamp import randomtimestamp
from confluent_kafka.admin import AdminClient, NewTopic


kafka_server = {"bootstrap.servers": "127.0.0.1:9092"}
admin_client = AdminClient(kafka_server)


def topic_creator(topic_details: List[Dict]):
    topic_list = [NewTopic(topic=topic['name'], num_partitions=topic['partition']) for topic in topic_details]
    print(topic_list)
    fs = admin_client.create_topics(topic_list)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        pass
        print('Message delivered to {}'.format(msg.topic()))


def confluent_kafka_producer():
    producer = Producer(kafka_server)
    for user_id in range(1, 11):
        record_key = user_id
        account_created_date = randomtimestamp(
            start_year=2020, end_year=2023, pattern="%Y-%m-%d %H:%M:%S")
        account_balance = random.randint(0, 100_000_000)
        record_value = {
            "user_id": user_id,
            "account_created_date": account_created_date,
            "account_balance": account_balance
        }

        record_value_json = json.dumps(record_value, default=str)

        producer.produce(
            "pipeline_one_topic",
            key=str(record_key),
            value=record_value_json,
            on_delivery=delivery_report
        )

    producer.flush()


topic_configs = [
    {'name': 'pipeline_one_topic', 'partition': 3},
    {'name': 'pipeline_two_topic', 'partition': 3}
]
topic_creator(topic_configs)
confluent_kafka_producer()
