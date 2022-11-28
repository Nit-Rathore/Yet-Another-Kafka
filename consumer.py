import socket

class consumer():
	DEFAULT_CONFIG = {
		'header': 64,
		'port': 5050,
		'format': 'utf-8',
		'disconnect_msg': '!DISCONNECT',
		'server': 'localhost'
	}
	DEFAULT_CONFIG['ADDR'] = (DEFAULT_CONFIG['server'], DEFAULT_CONFIG['port'])

	def __init__(self):
		pass

	def connect(self):
		self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.client.connect(self.DEFAULT_CONFIG['ADDR'])
	
	def send(self, msg):
		message = msg.encode(self.DEFAULT_CONFIG['format'])
		self.client.send(str(len(message)).encode(self.DEFAULT_CONFIG['format']) + b' '*(self.DEFAULT_CONFIG['header'] - len(str(len(message)).encode(self.DEFAULT_CONFIG['format']))))
		self.client.send(message)
		ack = self.client.recv(2048).decode(self.DEFAULT_CONFIG['format'])
		print(ack)

	def disconnect(self):
		self.send(self.DEFAULT_CONFIG['disconnect_msg'])
		self.client.close()


if __name__ == '__main__':
	cons1 = consumer()
	cons1.connect()
	message = 'consumer'
	cons1.send(message)
	print("Testing github stuff")
	cons2 = consumer()
	cons2.connect()
	message = 'cons2 HELLO'
	cons2.send(message)

	cons1.disconnect()
	cons2.disconnect()