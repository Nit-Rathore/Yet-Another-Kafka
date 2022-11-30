from consumer_class import consumer
connection,channel,queue = consumer.create_connection()
consumer.subcribeTopic('cars',channel,queue,False)
consumer.subcribeTopic('Bikes',channel,queue,False)
print(f"2 Waiting for Messages")
channel.start_consuming()