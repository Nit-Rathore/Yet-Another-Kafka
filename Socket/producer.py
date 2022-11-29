import socket

class producer():
	DEFAULT_CONFIG = {
		'header': 64,
		'port': 9090,
		'format': 'utf-8',
		'disconnect_msg': '!DISCONNECT',
		'server': 'localhost'
	}

	DEFAULT_CONFIG['ADDR'] = (DEFAULT_CONFIG['server'], DEFAULT_CONFIG['port'])
	data = {'type':'Producer', 'topic':None, 'content':None}

	BROKER_CONFIG = {
		'port': 9092,
		'server': 'localhost'
	}
	BROKER_CONFIG['ADDR'] = (BROKER_CONFIG['server'], BROKER_CONFIG['port'])

	def __init__(self,port=None,topic=None,content=None):
		self.port = port
		self.topic = topic
		self.content = content

	def connect_broker(self):
		self.broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.broker.connect(self.BROKER_CONFIG['ADDR'])
		print("Broker connection established")

	def connect_zookeeper(self):
		self.zookeeper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.zookeeper.connect(self.DEFAULT_CONFIG['ADDR'])
		print("Zookeeper connection established")

	def send(self, topic, content):
		self.data.update({'topic':topic,'content':content})
		data_to_send=str(self.data).encode()
		metadata = eval(self.zookeeper.recv(2048).decode(self.DEFAULT_CONFIG['format']))
		for i in metadata['brokers']:
			if(self.topic in i['leader_topics']):
				self.BROKER_CONFIG['port'] = i['port']
				self.BROKER_CONFIG['ADDR'] = (self.BROKER_CONFIG['server'], self.BROKER_CONFIG['port'])
		self.zookeeper.close()
		
		self.connect_broker()
		self.broker.sendall(data_to_send)
		ack = self.broker.recv(2048).decode(self.DEFAULT_CONFIG['format'])
		print(ack)
		self.broker.close()

		self.connect_zookeeper()
		self.BROKER_CONFIG['port'] = 9092
		self.BROKER_CONFIG['ADDR'] = (self.BROKER_CONFIG['server'], self.BROKER_CONFIG['port'])
		

	def disconnect(self):
		self.send(self.DEFAULT_CONFIG['disconnect_msg'])
		self.client.close()

if __name__ == '__main__':
    prod = producer()
    prod.connect_zookeeper()
    while True:
        a=int(input("Enter 0 to exit, anything else to send content: "))
        if a == 0:
            break
        else:
            topic = input("Enter topic: ")
            content = input("Enter content on topic: ")
            prod.send(topic, content)
