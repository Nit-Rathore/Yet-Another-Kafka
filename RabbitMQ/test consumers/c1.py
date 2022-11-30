from consumer_class import consumer
connection,channel,queue = consumer.create_connection()
consumer.subcribeTopic('cars',channel,queue,False)
print(f"1 Waiting for Messages")
channel.start_consuming()