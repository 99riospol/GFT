from kafka import KafkaConsumer

topic = 'simpsons-quotes'
bootstrap_servers = 'docker_test-kafka-1:29092'
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

for msg in consumer:
    print(msg.value.decode("utf-8"))