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

	def __init__(self):
		pass
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
		self.zookeeper.send(str(self.data['type']).encode('utf-8'))
		metadata = eval(self.zookeeper.recv(2048).decode(self.DEFAULT_CONFIG['format']))
		print(metadata)
		for i in metadata['brokers'].keys():
			if(topic in metadata['brokers'][i]['leader_topics']):
				self.BROKER_CONFIG['port'] = metadata['brokers'][i]['port']
				self.BROKER_CONFIG['ADDR'] = (self.BROKER_CONFIG['server'], self.BROKER_CONFIG['port'])
		print(self.BROKER_CONFIG)
		self.zookeeper.close()

		self.connect_broker()
		self.broker.send(data_to_send)
		# try:
		# except:
		# 	self.broker.sendall(data_to_send)
		self.broker.close()
		print("Broker connection closed")
		self.BROKER_CONFIG['port'] = 9092
		self.BROKER_CONFIG['ADDR'] = (self.BROKER_CONFIG['server'], self.BROKER_CONFIG['port'])
		

	def disconnect(self):
		self.send(self.DEFAULT_CONFIG['disconnect_msg'])
		self.client.close()

if __name__ == '__main__':
	prod = producer()
	while True:
		a=int(input("Enter 0 to exit, anything else to send content: "))
		if a == 0:
			break
		else:
			topic = input("Enter topic: ")
			content = input("Enter content on topic: ")
			prod.connect_zookeeper()
			prod.send(topic, [content])
