import pika
import time

from pika.exchange_type import ExchangeType
connection_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()
queue = channel.queue_declare(queue='', durable=True,auto_delete=False)
channel.exchange_declare(exchange='ui', exchange_type=ExchangeType.direct)
channel.queue_bind(exchange='ui',queue=queue.method.queue,routing_key='yoyo')
var =1
while True:
    var=var+1
    channel.basic_publish(exchange='ui' ,routing_key="yoyo",body = f"{var}")
    time.sleep(1)

