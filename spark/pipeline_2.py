from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pymongo import MongoClient
import datetime


class MongoUpsert:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.db = self.mongo_client["bootcamp"]
        self.collection = self.db["eligible_customers"]
        self.threshold_date = datetime.datetime(2023, 1, 1)
        self.threshold_balance = 90_000_000

    def upsert_to_mongo(self, batch_df, batch_id):
        for row in batch_df.rdd.toLocalIterator():
            user_id = row['value']["user_id"]
            account_created_date = row['value']["account_created_date"]
            account_balance = row['value']["account_balance"]
            
            if account_created_date < self.threshold_date or account_balance < self.threshold_balance:
                continue

            filter_query = {"user_id": user_id}
            update_query = {
                "$set": {
                    "account_created_date": account_created_date,
                    "account_balance": account_balance
                }
            }

            self.collection.update_one(filter_query, update_query, upsert=True)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Pipeline 2") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
        .getOrCreate()

    schema = StructType(
        [
            StructField("user_id", IntegerType()),
            StructField("account_created_date", TimestampType()),
            StructField("account_balance", IntegerType())
        ]
    )
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "account_balance") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

    mongo_upsert = MongoUpsert()

    query = value_df.writeStream \
        .foreachBatch(mongo_upsert.upsert_to_mongo) \
        .outputMode("update") \
        .trigger(processingTime="3 seconds") \
        .start()

    query.awaitTermination()
