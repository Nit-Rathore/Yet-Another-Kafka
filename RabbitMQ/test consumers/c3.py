from consumer_class import consumer
connection,channel,queue = consumer.create_connection()
consumer.subcribeTopic('Bikes',channel,queue,False)
print(f"3 Waiting for Messages")
channel.start_consuming()