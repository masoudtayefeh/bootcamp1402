import time
import random
from time import strftime
from confluent_kafka import Producer
import json
from randomtimestamp import randomtimestamp
from datetime import datetime, timedelta


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {}'.format(msg.topic()))


def confluent_kafka_producer():
    p = Producer({'bootstrap.servers': '127.0.0.1:9092'})
    for user_id in range(1, 1001):
        record_key = str(user_id)
        login_created_at = strftime(str(randomtimestamp(start_year=2020, end_year=2021, pattern="%Y-%m-%d %H:%M:%S")))
        transactions_created_at = strftime(str(datetime.strptime(login_created_at, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=10)))

        # Topic: users_app_login
        # Schema: {
        # 	login_id : Int,
        #   user_id : Int,
        #   created_at : timestamp
        # }
        login_value = {"user_id": user_id,
                       "login_id": user_id + 1400,
                       "login_at": login_created_at}

        # Topic: transactions
        # Schema: {
        # 	transaction_id : Int,
        #   user_id : Int,
        #   created_at : timestamp,
        #   amount: Int
        # }
        transactions_value = {"user_id": user_id,
                              "transaction_id": user_id + 1000000,
                              "created_at": transactions_created_at,
                              "amount": random.randint(1000, 1000000)}

        login_value = json.dumps(login_value)
        transactions_value = json.dumps(transactions_value)

        # print(login_value)
        # print(transactions_value)

        time.sleep(1)

        if user_id not in [11, 50, 103, 179, 254, 290, 390, 490, 600, 700, 800, 900]:
            p.produce("users_app_login", key=record_key, value=login_value, on_delivery=delivery_report)
        p.produce("transactions", key=record_key, value=transactions_value, on_delivery=delivery_report)
        p.poll(0)

    p.flush()


confluent_kafka_producer()
