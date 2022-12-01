import socket
import time
import threading
import os
import redis

class Broker():
	config = {
		'id': 3,
		'HEADER': 64,
		'PORT': 9094,
		'SERVER': "localhost",
		'FORMAT': 'utf-8',
		'DISCONNECT_MESSAGE': "!DISCONNECT"
	}
	metadata = {
    	'topics': ['India','F1','FIFA','Cricket','Cottons'],
    	'client_conn': {},
    	'server_conn': {},
    	'brokers': {
        	'1': {
            	'port': 9092,
            	'leader_topics': ['India','F1','FIFA']
        	},
        	'2':{
            	'port': 9093,
            	'leader_topics': ['Cricket']            
        	},
        	'3': {
            	'port': 9094,
            	'leader_topics': ['Cottons']
        	}
    	}
	}
	consumer_conns = []
	def metadata_receive(self) -> None:
		msg = self.zooclient.recv(2048).decode(self.config['FORMAT'])
		#self.metadata = eval(msg)
		#print(self.metadata)

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
		self.zooclient.send("broker1".encode(self.config['FORMAT']))
		self.metadata_receive()
		
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
			self.consumer_conns.append([client_data,conn,addr])
			self.consumer_send(client_data,conn,addr)
		elif(client_data['type'] == 'Producer'):
			self.producer_recv(client_data,conn,addr)
			for i in self.consumer_conns:
				if(i[0]['topic'] == client_data['topic']):
					self.consumer_send(i[0],i[1],i[2])
					
	def consumer_send(self,consumer_data,conn,addr): 
		broker_path = f"broker_{self.config['id']}/{consumer_data['topic']}"
		broker_path1 = broker_path[0:7] + '1' + broker_path[8:]
		broker_path2 = broker_path[0:7] + '2' + broker_path[8:]
		broker_path3 = broker_path[0:7] + '3' + broker_path[8:]
		
		consumer_msg = ""
		self.metadata = eval(r.get("metadata"))
		if(consumer_data['topic'] not in self.metadata['topics']): 
			os.makedirs(broker_path1)
			os.makedirs(broker_path2)
			os.makedirs(broker_path3)
			self.metadata['topics'].append(consumer_data['topic'])
			self.metadata['brokers'][str(self.config['id'])]['leader_topics'].append(consumer_data['topic'])
		files = os.listdir(broker_path)
		if(consumer_data['offset']==-1): 
			consumer_data['offset'] = len(os.listdir(broker_path))
		while consumer_data['offset']<len(os.listdir(broker_path)):
			consumer_data['offset'] += 1
			file_name = os.path.join(broker_path, files[consumer_data['offset']-1])
			with open(file_name,"r") as myFile:
				consumer_msg += myFile.read()+'| '
		consumer_topic_msg = str(tuple((consumer_data['topic'], consumer_msg)))
		conn.send(consumer_topic_msg.encode(self.config['FORMAT']))

		r.set("metadata", str(self.metadata))

	def producer_recv(self,producer_data,conn,addr): 
		self.metadata = eval(r.get("metadata"))
		broker_path = f"broker_{self.config['id']}/{producer_data['topic']}"
		broker_path1 = broker_path[0:7] + '1' + broker_path[8:]
		broker_path2 = broker_path[0:7] + '2' + broker_path[8:]
		broker_path3 = broker_path[0:7] + '3' + broker_path[8:]

		if(producer_data['topic'] not in self.metadata['topics']): 
			print("Dir created")
			os.makedirs(broker_path1)
			os.makedirs(broker_path2)
			os.makedirs(broker_path3)
			self.metadata['topics'].append(producer_data['topic'])
			self.metadata['brokers'][str(self.config['id'])]['leader_topics'].append(producer_data['topic'])

		data = (producer_data['topic'], producer_data['content'])
		nfiles = len(os.listdir(broker_path))
		len_data = len(data[1])
		i = 0
		while(len_data - 3 >= 0):
			with open(f'{broker_path1}\{nfiles+1}.txt', 'w') as f:
				f.write('\n'.join(data[1][i:i+3]))
			with open(f'{broker_path2}\{nfiles+1}.txt', 'w') as f:
				f.write('\n'.join(data[1][i:i+3]))
			with open(f'{broker_path3}\{nfiles+1}.txt', 'w') as f:
				f.write('\n'.join(data[1][i:i+3]))
			i+=3
			len_data-=3
			nfiles += 1
		if(len_data!=0):
			with open(f'{broker_path1}\{nfiles+1}.txt','w') as f:
				f.write('\n'.join(data[1][i:]))
			with open(f'{broker_path2}\{nfiles+1}.txt','w') as f:
				f.write('\n'.join(data[1][i:]))
			with open(f'{broker_path3}\{nfiles+1}.txt','w') as f:
				f.write('\n'.join(data[1][i:]))

		r.set("metadata", str(self.metadata))

if __name__ == '__main__':
	r = redis.Redis(host='localhost', port=6379, decode_responses=True)

	Brokerx = Broker(9094)