import pika
from pika.exchange_type import ExchangeType
import os
from zookeeper_rabbit import get_leadership_info

connection_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()
channel.exchange_declare(exchange='routing', exchange_type=ExchangeType.direct)
queue = channel.queue_declare(queue='', exclusive=True)


def open_required_queues():    
    lino = get_leadership_info()
    for topic in lino:
        num_part = lino[topic][0]
        for i in range(1,num_part+1):
            if(lino[topic][i]==2):
                channel.queue_bind(exchange='routing', queue=queue.method.queue, routing_key=f'{topic}/partition{i}')   
    return channel,queue


def on_message_received(ch, method, properties, body): 
    print(f"Third Broker - received new message: {body}")
    os.makedirs(f'br3/{method.routing_key}', exist_ok=True)
    f = open(f"{method.routing_key}/log.txt", "w")
    f.write(f"{body}")
    f.close()



open_required_queues()

channel.basic_consume(queue=queue.method.queue, auto_ack=True,
    on_message_callback=on_message_received)

print("Broker 3 running")

channel.start_consuming()