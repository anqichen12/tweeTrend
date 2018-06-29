from kafka import KafkaConsumer
topic_name = "twitter"
consumer = KafkaConsumer(bootstrap_servers='*')
consumer.subscribe([topic_name])
for message in consumer:
 	print (message)
