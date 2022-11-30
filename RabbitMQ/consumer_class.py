import pika
from pika.exchange_type import ExchangeType
from zookeeper_rabbit import actively_consumed_topics
from redis import Redis
import json,random
cli = Redis('localhost')


class consumer():
       
    def create_connection():
       connection_parameters = pika.ConnectionParameters('localhost')
       connection = pika.BlockingConnection(connection_parameters)
       channel = connection.channel()
       queue = channel.queue_declare(queue='', exclusive=True)
       channel.exchange_declare(exchange='c', exchange_type=ExchangeType.direct)
       
       return connection,channel,queue
   
    def subcribeTopic(topicName,channel,queue,fromBeginning):
       if(not fromBeginning):
         def omr(ch, method, properties, body):
            print(f'Recieved {body}')
         actively_consumed_topics(topicName)
         channel.queue_bind(exchange='c',queue=queue.method.queue,routing_key=f'{topicName}')
         channel.basic_consume(queue=queue.method.queue, auto_ack=True,on_message_callback=omr)
         print(f"Subscribed to {topicName}")
       

       

       
