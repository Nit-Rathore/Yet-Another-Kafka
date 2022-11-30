import pika
from redis import Redis
import json,random,signal
from pika.exchange_type import ExchangeType
connection_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()
queue = channel.queue_declare(queue='', durable=True,auto_delete=False)
channel.exchange_declare(exchange='ui', exchange_type=ExchangeType.direct)
channel.queue_bind(exchange='ui',queue=queue.method.queue,routing_key='yoyo')

def omr(ch, method, properties, body):
            
    cli = Redis('localhost')
    topicName = method.routing_key
    log = json.loads(cli.get('log'))
    if(topicName in log.keys()):
        messages=log[topicName]
        messages.append(body.decode())
        log[topicName]=messages
        cli.set("log",json.dumps(log))
    else:
        messages=[]
        messages.append(body.decode())
        log[topicName]=messages
        cli.set("log",json.dumps(log))
    print(log)
    
                
channel.basic_consume(queue=queue.method.queue,auto_ack=True,on_message_callback=omr)
channel.start_consuming()