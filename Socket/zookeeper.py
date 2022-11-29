import socket 
import threading
import time

HEADER = 64
PORT = 9090
SERVER = "localhost"
ADDR = ('localhost', PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"

zooserver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
zooserver.bind(ADDR)

metadata = {
    'topics': {},
    'client_conn': {},
    'server_conn': {},
    'brokers': {
        '1': {
            'port': 9092,
            'leader_topics': []
        },
        '2':{
            'port': 9093,
            'leader_topics': []            
        },
        '3': {
            'port': 9094,
            'leader_topics': []
        }
    }
}

def metadata_transfer(conn, addr):
    msg = str(metadata).encode(FORMAT)
    conn.send(msg)

def heartbeating(conn, addr):
    connected = True
    while connected:
        msg = conn.recv(2048).decode(FORMAT)
        print(msg)

def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    metadata_transfer(conn, addr)

    threading.Thread(target = heartbeating, args=(conn, addr)).start()

    time.sleep(7)
    metadata_transfer(conn,addr)

        

def start():
    zooserver.listen()
    print(f"[LISTENING] Server is listening on {SERVER}:{PORT}")
    while True:
        conn, addr = zooserver.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")


print("[STARTING] server is starting...")
start()