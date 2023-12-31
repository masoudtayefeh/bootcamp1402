from confluent_kafka import Consumer
from mongoengine import connect, Document, fields
import json
import datetime

consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'consumer_one',
    'auto.offset.reset': 'earliest'
})

connect(
    host="mongodb://localhost:27017/bootcamp",
    db='bootcamp'
)

consumer.subscribe(['pipeline_one_topic'])

class EligibleCustomers(Document):
    user_id = fields.IntField(unique=True)
    account_created_date = fields.DateTimeField()
    account_balance = fields.IntField()

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    try:
        data = json.loads(msg.value())
        print('Received message: {}'.format(data))
        
        account_created_date = datetime.datetime.strptime(data['account_created_date'], '%Y-%m-%d %H:%M:%S')
        threshold_date = datetime.datetime(2023, 1, 1)
        threshold_balance = 90_000_000

        if account_created_date > threshold_date and data['account_balance'] > threshold_balance:
            EligibleCustomers.objects(user_id=data['user_id']).update_one(
                upsert=True,
                set__account_created_date=data['account_created_date'],
                set__account_balance=data['account_balance']
            )
    except Exception as e:
        print("Error processing message:", e)

consumer.close()
