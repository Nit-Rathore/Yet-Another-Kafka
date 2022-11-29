import socket 
import threading

HEADER = 64
PORT = 5050
SERVER = "localhost"
ADDR = ('localhost', PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    msg_length = conn.recv(HEADER).decode(FORMAT)
    if msg_length:
        msg_length = int(msg_length)
        msg = conn.recv(msg_length).decode(FORMAT)
        if msg == "producer":
            print("Producer connected")
            conn.send("Connected".encode(FORMAT))
            connected = True
            while connected:
                msg_length = conn.recv(HEADER).decode(FORMAT)
                if msg_length:
                    msg_length = int(msg_length)
                    msg = conn.recv(msg_length).decode(FORMAT)
                    if msg == DISCONNECT_MESSAGE:
                        connected = False
                    print(f"[{addr}] {msg}")
                    if msg == DISCONNECT_MESSAGE:
                        conn.send("Disconnected".encode(FORMAT))
                    else:
                        conn.send("Msg received".encode(FORMAT))

        elif msg == "consumer":
            print("Consumer connected")
            conn.send("Connected".encode(FORMAT))
            connected = True
            while connected:
                msg_length = conn.recv(HEADER).decode(FORMAT)
                if msg_length:
                    msg_length = int(msg_length)
                    msg = conn.recv(msg_length).decode(FORMAT)
                    if msg == DISCONNECT_MESSAGE:
                        connected = False
                    print(f"[{addr}] {msg}")
                    if msg == DISCONNECT_MESSAGE:
                        conn.send("Disconnected".encode(FORMAT))
                    else:
                        conn.send("Msg received".encode(FORMAT))

        elif msg == DISCONNECT_MESSAGE:
            print(f"[{addr}] {msg}")
            conn.send("Disconnected".encode(FORMAT))
        else:
            print("Wrong formatting of first message.")
            conn.send("Incorrect first message. Disconnected.")
    conn.close()

def start():
    server.listen()
    print(f"[LISTENING] Server is listening on {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")


print("[STARTING] server is starting...")
start()