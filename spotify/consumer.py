from kafka import KafkaConsumer
topic_name = "spotify"
consumer = KafkaConsumer(enable_auto_commit=False, bootstrap_servers='*')
consumer.subscribe([topic_name])
for message in consumer:
 	print (message.value)
        print(message.offset)
