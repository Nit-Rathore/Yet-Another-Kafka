import socket 
import threading

class Broker():
	config = {
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

	def __init__(self, port=None) -> None:
		self.config['PORT'] = int(port)
		self.config['ADDR'] = (self.config['SERVER'], self.config['PORT'])
		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server.bind(self.config['ADDR'])
		zooclient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		zooclient.connect((self.config['SERVER'], 9090))

		while True:
			msg = zooclient.recv(2048).decode(self.config['FORMAT'])
			print(eval(msg))
			msg = input()
			zooclient.send(msg.encode(self.config['FORMAT']))
			if(msg == 'exit'):
				break
		
		zooclient.close()


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
	Brokerx = Broker(port)
	# Broker2 = Broker(9093)
	# Broker3 = Broker(9094)