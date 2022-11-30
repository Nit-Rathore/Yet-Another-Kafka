import pika
from pika.exchange_type import ExchangeType
import os,signal,random
from redis import Redis
import json
cli = Redis('localhost')
from zookeeper_rabbit import notify_brokers

connection_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_parameters)
consume = connection.channel()
qconsume = consume.queue_declare(queue='', exclusive=True)
consume.exchange_declare(exchange='c', exchange_type=ExchangeType.direct)

def on_broker_failure(broker_no):
    print(f"Broker {broker_no}failed off bro")
    lino = json.loads(cli.get('leadership'))
    active_brokers = json.loads(cli.get('active_brokers'))
    active_brokers.remove(broker_no) 
    for topic in lino:
        num_part = lino[topic][0]
        for i in range(1,num_part+1):
            if(lino[topic][i]==broker_no):
                lino[topic][i]=random.choice(active_brokers)
    cli.set('leadership',json.dumps(lino))
    cli.set('active_brokers',json.dumps(active_brokers))
    notify_brokers()

def keyboardInterruptHandler(signal, frame):
    on_broker_failure(2)
    exit(0)
signal.signal(signal.SIGINT, keyboardInterruptHandler)   




channel = connection.channel()
queue = channel.queue_declare(queue='', exclusive=True)
channel.exchange_declare(exchange='routing', exchange_type=ExchangeType.direct)
channel.queue_bind(exchange='routing',queue=queue.method.queue,routing_key='zookeeper')


channel2 = connection.channel()
queue2 = channel.queue_declare(queue='', exclusive=True)
channel2.exchange_declare(exchange='routing2', exchange_type=ExchangeType.direct)



def open_required_queues():    
    lino = json.loads(cli.get('leadership')) 
    for topic in lino:
        num_part = lino[topic][0]
        for i in range(1,num_part+1):
            if(lino[topic][i]==2):
                channel.queue_bind(exchange='routing', queue=queue.method.queue, routing_key=f'2/{topic}/partition{i}')   
            else:
                channel2.queue_bind(exchange='routing2', queue=queue2.method.queue, routing_key=f'2/{topic}/partition{i}')
                
                

def on_message_received(ch, method, properties, body): 
    if(method.routing_key=="zookeeper"):
        print(f"Broker - received new message: {body}")
        open_required_queues()
    else:
        
            print(f"Broker - received new message: {body}")
            os.makedirs(f'{method.routing_key}', exist_ok=True)
            f = open(f"{method.routing_key}/log.txt", "a")
            f.write(f'{body}\n')
            f.close() 
            channel2.basic_publish(exchange='routing2', routing_key="3"+method.routing_key[1:], body=body)
            channel2.basic_publish(exchange='routing2', routing_key="1"+method.routing_key[1:], body=body)
            topicName = ((method.routing_key[2:]).split('/'))[0]
            if(topicName in json.loads(cli.get('consumed')) and 2 == json.loads(cli.get('consumed'))[topicName] ):
                consume.basic_publish(exchange='c',routing_key=topicName,body=body) 
        

def omr(ch, method, properties, body):
            os.makedirs(f'{method.routing_key}', exist_ok=True)
            f = open(f"{method.routing_key}/log.txt", "a")
            f.write(f'{body}\n')
            f.close() 
            topicName = ((method.routing_key[2:]).split('/'))[0]
            if(topicName in json.loads(cli.get('consumed')) and 2 == json.loads(cli.get('consumed'))[topicName] ):
                consume.basic_publish(exchange='c',routing_key=topicName,body=body) 

channel.basic_consume(queue=queue.method.queue, auto_ack=True,
    on_message_callback=on_message_received)

channel2.basic_consume(queue=queue2.method.queue, auto_ack=True,
    on_message_callback=omr)

print("Broker 2 running")

channel.start_consuming()
channel2.start_consuming()