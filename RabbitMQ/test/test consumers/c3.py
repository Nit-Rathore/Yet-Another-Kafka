from consumer_class import consumer
connection,channel,queue = consumer.create_connection()
print("Getting all messages")
consumer.subcribeTopic('Bikes',channel,queue,False)

channel.start_consuming()