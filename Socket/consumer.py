#Implementing the consumer as a client architecture to pass the data from the broker to the client.

import socket
import threading 
import sys

class consumer():
	DEFAULT_CONFIG = {
		'header': 64,
		'port': 9090,
		'format': 'utf-8',
		'disconnect_msg': '!DISCONNECT',
		'server': 'localhost'
	}
	DEFAULT_CONFIG['ADDR'] = (DEFAULT_CONFIG['server'], DEFAULT_CONFIG['port'])
	
	BROKER_CONFIG ={
		'port': 9092,
		'server': 'localhost'
	}
	
	BROKER_CONFIG['ADDR'] = (BROKER_CONFIG['server'], BROKER_CONFIG['port'])

	data = {'type' : 'Consumer', 'topic': None, 'offset': -1} #Not-beginning == 0, -from-beginning ==1 

	def __init__(self,topic=None,flag=None,port =9090):
		self.data['topic'] = topic
		self.DEFAULT_CONFIG['port'] = port

		if(flag == 1):
			self.data['offset'] = 0

    	
	def connect_broker(self):
		self.broker_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.broker_client.connect(self.BROKER_CONFIG['ADDR'])
		print("Connection with broker has been established")
		self.broker_client.send(str(self.data).encode(self.DEFAULT_CONFIG['format']))

	
	def connect_zookeeper(self): 
		
		self.zoo_client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		self.zoo_client.connect(self.DEFAULT_CONFIG['ADDR'])
		self.zoo_client.send(self.data['type'].encode(self.DEFAULT_CONFIG['format'])) ##Sending acknowledgement to ZooKeeper
		
		print("Connection with Zookeeper has been established ...")
		
		metadata = eval(self.zoo_client.recv(2048).decode(self.DEFAULT_CONFIG['format']))

		for i in metadata['brokers']:
			if(self.data['topic'] in metadata['brokers'][i]['leader_topics']): 
				self.BROKER_CONFIG['port'] =  metadata['brokers'][i]['port']
				self.BROKER_CONFIG['ADDR'] = (self.BROKER_CONFIG['server'], self.BROKER_CONFIG['port'])
		
		print(self.BROKER_CONFIG['ADDR'])
		self.zoo_client.close()
		

	def send_recieve(self):  ##To send the broker my data received from user and receive data from broker accordingly...
		print("Message received from broker...")
		message = eval(self.broker_client.recv(18432).decode(self.DEFAULT_CONFIG['format']))
		print(message) 
		#self.client.send("Message recieved from leader broker".encode())


	def disconnect(self):
		self.broker_client.send(self.DEFAULT_CONFIG['disconnect_msg'])
		self.broker_client.close()
		

if __name__ == "__main__":
	topic = sys.argv[1]
	flag = sys.argv[2]
	cons1 = consumer(topic,int(flag))
	cons1.connect_zookeeper()
	try:
		cons1.connect_broker()
		while True:
			cons1.send_recieve()
	# 	cons1.disconnect()

	except:
		pass
	 	#cons1.connect_zookeeper()
