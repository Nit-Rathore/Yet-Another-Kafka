import socket 
import threading
import time
import redis


HEADER = 64
PORT = 9090
SERVER = "localhost"
ADDR = ('localhost', PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

zooserver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
zooserver.bind(ADDR)

metadata = {
    'topics': ['India','F1','FIFA','Cricket','Cottons'],
    'client_conn': {},
    'server_conn': {},
    'brokers': {
        '1': {
            'port': 9092,
            'leader_topics': ['India','F1','FIFA'],
            'status': 'alive'
        },
        '2':{
            'port': 9093,
            'leader_topics': ['Cricket'],
            'status': 'alive'
        },
        '3': {
            'port': 9094,
            'leader_topics': ['Cottons'],
            'status': 'alive'
        }
    }
}

r.set("metadata", str(metadata))

def metadata_transfer(conn, addr):
    metadata = eval(r.get("metadata"))
    msg = str(metadata).encode(FORMAT)
    conn.send(msg)

def metadata_recv(self, conn, addr):
    msg = eval(conn.recv(2048).decode(FORMAT))
    # self.metadata.update(msg)
    # print(self.metadata)

def heartbeating(conn, addr, id):
    connected = True
    while connected:
        try:
            msg = conn.recv(2048).decode(FORMAT)
            print(msg)
        except Exception as e:
            print("Heartbeat accept failed! Sleeping for 5 seconds, then trying connection again...")
            time.sleep(5)
            try:
                msg = conn.recv(2048).decode(FORMAT)
                print(msg)
            except Exception as e:
                print("Heartbeat accept failed 2 times! Sleeping for 5 seconds, then trying last connection request again...")
                time.sleep(5)
                try:
                    msg = conn.recv(2048).decode(FORMAT)
                    print(msg)
                except Exception as e:
                    print("Connection Failed !!! Disconnecting Broker and treating it as dead...")
                    conn.close()
                    metadata = eval(r.get("metadata"))
                    # print(metadata['brokers'][str(id)])
                    # # print(id, type(id))
                    metadata['brokers'][str(id)]['status'] = 'dead'
                    leader_election(metadata, id)
                    metadata = r.set("metadata", str(metadata))
                    break
                    
def leader_election(metadata, id):
    dead_topics = metadata['brokers'][id]['leader_topics']
    metadata['brokers'][id]['leader_topics'] = []
    for i in metadata['brokers']:
        if metadata['brokers'][i]['status'] == 'alive':
            metadata['brokers'][i]['leader_topics'].extend(dead_topics)
            break
    print(metadata)
    r.set("metadata", str(metadata))

def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    type = conn.recv(2048).decode(FORMAT)
    id = str(type[-1])
    print(type)
    if(type[:-1] == "broker"):
        metadata_transfer(conn, addr)

        threading.Thread(target = heartbeating, args=(conn, addr, id)).start()
    else:
        metadata_transfer(conn,addr)
        #conn.close()

        

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