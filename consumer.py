#Implementing the consumer as a client architecture to pass the data from the broker to the client.

import socket
import threading 
import sys
import json 

class consumer():
	DEFAULT_CONFIG = {
		'header': 64,
		'port': 5050,
		'format': 'utf-8',
		'disconnect_msg': '!DISCONNECT',
		'server': 'localhost'
	}
	DEFAULT_CONFIG['ADDR'] = (DEFAULT_CONFIG['server'], DEFAULT_CONFIG['port'])
	data = {'type' : 'Consumer', 'topic': None, 'flag': 0} #Not-beginning == 0, -from-beginning ==1 

	def __init__(self,topic=None,flag=None):
		self.topic = topic
		self.flag = flag

	def connect(self):
		self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.client.connect(self.DEFAULT_CONFIG['ADDR'])
		 
	def recieve(self, topic,msg):  ##To send the broker my data received from user and receive data from broker accordingly...
		self.data.update({'topic':self.topic, 'flag':self.flag})
		data_string = str.encode(json.dumps(self.data)) #data serialized
		#data_loaded = json.loads(self.data) #Not sure if this is required or not ...
		self.client.sendall(data_string) #Sending the metadata to the broker ....
		message = self.client.recv(2048).decode(self.DEFAULT_CONFIG['format'])
		if(len(message)): print(message) , self.client.send("Message recieved from leader broker".encode())

		'''
		message = msg.encode(self.DEFAULT_CONFIG['format'])
		self.client.send(str(len(message)).encode(self.DEFAULT_CONFIG['format']) + b' '*(self.DEFAULT_CONFIG['header'] - len(str(len(message)).encode(self.DEFAULT_CONFIG['format']))))
		self.client.send(message)
		ack = self.client.recv(2048).decode(self.DEFAULT_CONFIG['format'])
		print(ack)'''

	def disconnect(self):
		self.send(self.DEFAULT_CONFIG['disconnect_msg'])
		self.client.close()
		

if __name__ == "__main__":
    
	topic = sys.argv[1]
	flag = sys.argv[2]
	cons1 = consumer(topic,flag)
	cons1.connect()
	cons1.disconnect()
	