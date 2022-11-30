import pika
from pika.exchange_type import ExchangeType
from zookeeper_rabbit import set_leader,get_leader
from redis import Redis
import json
cli = Redis('localhost')

class producer():
    def createTopic(topicName,num_partition):
       set_leader(topicName=topicName,num_part=num_partition)
       
    def create_connection(topicName):
       connection_parameters = pika.ConnectionParameters('localhost')
       connection = pika.BlockingConnection(connection_parameters)
       channel = connection.channel()
       channel.exchange_declare(exchange='routing', exchange_type=ExchangeType.direct)
       queue = channel.queue_declare(queue='', exclusive=True)
       return connection,channel
    
    def publish(topicName,key,value,channel):
       part,leader = get_leader(topicName,key)
       routing_queue_name = f'{leader}/{topicName}/partition{part}'
       print(f'sent message: {key}:{value} {routing_queue_name}')
       channel.basic_publish(exchange='routing', routing_key=routing_queue_name, body=f"{key}:{value}")
       
    def close(connection):
        connection.close() 
       


    

