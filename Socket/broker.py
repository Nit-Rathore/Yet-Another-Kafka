import socket
import time
import threading

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
		msg = f"broker{self.config['id']} alive"
		self.zooclient.send(msg.encode(self.config['FORMAT']))

	def __init__(self, port, id) -> None:
		self.config['id'] = id
		self.config['PORT'] = int(port)
		self.config['ADDR'] = (self.config['SERVER'], self.config['PORT'])
		self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server.bind(self.config['ADDR'])
		self.zooclient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.zooclient.connect((self.config['SERVER'], 9090))
		self.metadata_receive()
		
		self.heartbeat()

		self.metadata_receive()





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
	id = int(input())
	Brokerx = Broker(port, id)
	# Broker2 = Broker(9093)
	# Broker3 = Broker(9094)