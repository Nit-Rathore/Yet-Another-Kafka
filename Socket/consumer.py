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

	data = {'type' : 'Consumer', 'topic': None, 'flag': 0} #Not-beginning == 0, -from-beginning ==1 

	def __init__(self,topic=None,flag=None,port =9090):
		self.topic = topic
		self.flag = flag
		self.DEFAULT_CONFIG['port'] = port

    	
	def connect_broker(self):
		self.broker_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.broker_client.connect(self.BROKER_CONFIG['ADDR'])
		print("Connection with broker has been established")

	
	def connect_zookeeper(self): 
		
		self.zoo_client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		self.zoo_client.connect(self.DEFAULT_CONFIG['ADDR'])
		self.zoo_client.send(str(self.data).encode()) ##Sending acknowledgement to ZooKeeper
		
		print("Connection with Zookeeper has been established ...")
		
		metadata = eval(self.zoo_client.recv(2048).decode(self.DEFAULT_CONFIG['format']))

		for i in metadata['brokers']:  
			if(self.data['topic'] in i['leader_topics']): 
				self.BROKER_CONFIG['port'] = i['port']
				self.BROKER_CONFIG['ADDR'] = (self.BROKER_CONFIG['server'], self.BROKER_CONFIG['port'])
		
		self.zoo_client.close()
		

	def send_recieve(self, topic,msg):  ##To send the broker my data received from user and receive data from broker accordingly...

		self.data.update({'topic':self.topic, 'flag':self.flag})
		data_string = str(self.data).encode() 
		self.broker_client.sendall(data_string) #Sending the metadata to the broker ....
		
		print("Message received from broker...")
		message = self.broker_client.recv(2048).decode(self.DEFAULT_CONFIG['format'])
		
		if(len(message)): 
			print(message) 
			self.client.send("Message recieved from leader broker".encode())


	def disconnect(self):
		self.broker_client.send(self.DEFAULT_CONFIG['disconnect_msg'])
		self.broker_client.close()
		

if __name__ == "__main__":
	topic = sys.argv[1]
	flag = sys.argv[2]
	cons1 = consumer(topic,flag)
	#cons1.connect_zookeeper()
	
	while True:
		try:
			cons1.connect_broker()
			cons1.send_recieve()
			cons1.disconnect()

		except:
			pass
			#cons1.connect_zookeeper()                                           