import socket

class producer():
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
    prod1 = producer()
    prod1.connect()
    message = 'producer'
    prod1.send(message)
    while True:
        a=int(input("Enter 0 to exit, anything else to keep sending info: "))
        if a == 0:
            break
        else:
            msg = input("Enter topic: ")
            prod1.send(msg)
            msg2 = input("Enter info on topic: ")
            prod1.send(msg2)
    prod1.disconnect()