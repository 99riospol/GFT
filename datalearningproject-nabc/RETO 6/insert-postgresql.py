from kafka import KafkaConsumer
import json
import psycopg2

topic = 'simpsons-quotes'
bootstrap_servers = 'docker_test-kafka-1:29092'
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

conn = psycopg2.connect(
    host="docker_test-postgres-1",
    port="5432",
    database="simpsons",
    user="root",
    password="1234")

cursor = conn.cursor()

insert1 = "INSERT INTO public.characters (name,image) VALUES(%s,%s);"
insert2 = "INSERT INTO public.quotes (quote,\"character\",\"characterDirection\") VALUES(%s,%s,%s);"

for msg in consumer:
    if (msg.offset % 2) == 0:
        data = json.loads(msg.value.decode("utf-8"))
        cursor.execute(insert1,(data['character'], data['image']))
        cursor.execute(insert2,(data['quote'],data['character'],data['characterDirection']))
        conn.commit()

cursor.close()
