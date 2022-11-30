import pika
import os
from time import sleep
from pika.exchange_type import ExchangeType
from redis import Redis
import json
cli = Redis('localhost')

def set_leader(topicName,num_part):
    topic_leadership = json.loads(cli.get('leadership')) 
    leader_array = []
    leader_array.append(num_part)
    top=1
    for i in range(1,num_part+1):
        leader_array.append(top%3+1)
        top+=1
    topic_leadership[topicName]=leader_array
    cli.set('leadership',json.dumps(topic_leadership))
    notify_brokers()


def get_leader(topicName,key):
    topic_leadership = json.loads(cli.get('leadership')) 
    num_part= topic_leadership[topicName][0]
    part = hash(key)%(num_part) +1
    return part,topic_leadership[topicName][part]

def notify_brokers():
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='routing', exchange_type=ExchangeType.direct)
    channel.basic_publish(exchange='routing',routing_key='zookeeper',body="update your queues !")

if __name__ == "__main__":
    
    topic_leadership={}
    cli = Redis('localhost')
    s = json.dumps(topic_leadership)
    cli.set('leadership',s)
    print("Zookeeper is Running")
    while True: 
        topic_leadership = json.loads(cli.get('leadership'))
        print(topic_leadership)
        sleep(5)