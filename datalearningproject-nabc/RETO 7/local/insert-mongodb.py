import pymongo
from kafka import KafkaConsumer
import json

topic = 'simpsons-quotes'
bootstrap_servers = 'localhost:9092'
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

client=pymongo.MongoClient(username='root',password='1234')

simpsons_db=client["simpsons"]

quotes_col=simpsons_db["quotes"]

for msg in consumer:
    if (msg.offset % 2) == 0:
        quote = json.loads(msg.value.decode("utf-8"))
        quotes_col.insert_one(quote)

