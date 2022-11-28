import pika
from pika.exchange_type import ExchangeType
from zookeeper_rabbit import set_leader,get_leader
class producer():
    def createTopic(topicName,num_partition):
       set_leader(topicName=topicName,num_part=num_partition)
       
    def create_connection(topicName):
       connection_parameters = pika.ConnectionParameters('localhost')
       connection = pika.BlockingConnection(connection_parameters)
       channel = connection.channel()
       channel.exchange_declare(exchange='routing', exchange_type=ExchangeType.direct)
       queue = channel.queue_declare(queue='', exclusive=True)
       channel.queue_bind(exchange='routing', queue=queue.method.queue, routing_key=topicName)
       return connection,channel
    
    def publish(topicName,key,value,channel):
       leader = get_leader(key)
       routing_queue_name = f'{topicName}/partition{leader}'
       print(f'sent message: {key}:{value}')
       channel.basic_publish(exchange='routing', routing_key=routing_queue_name, body=f"{key}:{value}")
       
    def close(connection):
        connection.close() 
       


    

