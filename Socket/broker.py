import socket
import time
import threading
import os

class Broker():
	config = {
		'id': 1,
		'HEADER': 64,
		'PORT': 9092,
		'SERVER': "localhost",
		'FORMAT': 'utf-8',
		'DISCONNECT_MESSAGE': "!DISCONNECT"
	}
	metadata = {
		'id': 1,
		'topics': {},
		'consumer_conn': {},
		'producer_conn': {}
	}

	def metadata_receive(self) -> None:
		msg = self.zooclient.recv(2048).decode(self.config['FORMAT'])
		self.metadata = eval(msg)
		print(self.metadata)

	def heartbeat(self) -> None:
		threading.Timer(5.0, self.heartbeat).start()
		msg = f"broker{self.config['id']} is alive"
		self.zooclient.send(msg.encode(self.config['FORMAT']))

	def __init__(self, port) -> None:
		#self.config['id'] = id
		self.config['PORT'] = int(port)
		self.config['ADDR'] = (self.config['SERVER'], self.config['PORT'])
		self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server.bind(self.config['ADDR'])
		self.zooclient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.zooclient.connect((self.config['SERVER'], 9090))
		self.zooclient.send("broker".encode(self.config['FORMAT']))
		self.metadata_receive()
		for i in self.metadata['brokers']:
			if self.metadata['brokers'][i]['port'] == int(port):
				self.config['id']=int(i)
				print(self.config['id'])
				break
		self.heartbeat()
		self.server_start()

	def server_start(self): 
		self.server.listen()
		print(f"[LISTENING] Server is listening on {self.config['SERVER']}") 
		while True:
			conn,addr = self.server.accept()
			thread = threading.Thread(target=self.handle_client,args = (conn,addr))
			thread.start()
	
	def handle_client(self,conn,addr): 
		print(f"[NEW CONNECTION] {addr} connected.")
		client_data = eval(conn.recv(2048).decode()) 
		if(client_data['type'] == 'Consumer'): 
			print(client_data)
			self.consumer_send(client_data,conn,addr)
		elif(client_data['type'] == 'Producer'):
			self.producer_recv(client_data,conn,addr)

	def consumer_send(self,consumer_data,conn,addr): 
		broker_path = f"broker_{self.config['id']}/{consumer_data['topic']}"
		files = os.listdir(broker_path)
		consumer_msg = ""
		if(consumer_data['topic'] not in self.metadata['topics']): 
			os.makedirs(broker_path)
		if(consumer_data['offset']==-1): 
			consumer_data['offset'] = len(os.listdir(broker_path))
		while consumer_data['offset']<len(os.listdir(broker_path)):
			consumer_data['offset'] += 1
			file_name = os.path.join(broker_path, files[consumer_data['offset']-1])
			with open(file_name,"r") as myFile:
				consumer_msg += myFile.read()
		consumer_topic_msg = str(tuple((consumer_data['topic'], consumer_msg)))
		conn.send(consumer_topic_msg.encode(self.config['FORMAT'])) 

	def producer_recv(self,producer_data,conn,addr): 
		print(self.metadata)
		if(producer_data['topic'] not in self.metadata['topics']): 
			broker_path = f"broker_{self.config['id']}/{producer_data['topic']}"
			print("Dir created")
			os.makedirs(broker_path)
			self.metadata['topics'].append(producer_data['topic'])
			self.metadata['brokers'][self.config['id']]['leader_topics'].append(producer_data['topic'])
		else: 
			broker_path = f"broker_{self.config['id']}/{producer_data['topic']}" 
		#self.metadata_send()

	# def handle_client(conn, addr):
	# 	print(f"[NEW CONNECTION] {addr} connected.")

	# 	connected = True
	# 	while connected:
	# 		msg_length = conn.recv(HEADER).decode(FORMAT)
	# 		if msg_length:
	# 			msg_length = int(msg_length)
	# 			msg = conn.recv(msg_length).decode(FORMAT)
	# 			if msg == DISCONNECT_MESSAGE:
	# 				connected = False

	# 			print(f"[{addr}] {msg}")
	# 			conn.send("Msg received".encode(FORMAT))

	# 	conn.close()
			

	# def start():
	# 	server.listen()
	# 	print(f"[LISTENING] Server is listening on {SERVER}")
	# 	while True:
	# 		conn, addr = server.accept()
	# 		thread = threading.Thread(target=handle_client, args=(conn, addr))
	# 		thread.start()
	# 		print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")


	# print("[STARTING] server is starting...")
	# start()


if __name__ == '__main__':
	port = int(input())
	#id = int(input())
	Brokerx = Broker(port)
	# Broker2 = Broker(9093)
	# Broker3 = Broker(9094)