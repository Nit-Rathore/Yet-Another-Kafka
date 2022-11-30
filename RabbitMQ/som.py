from redis import Redis
import json,random,signal
from zookeeper_rabbit import notify_brokers

# cli = Redis('localhost')
# def on_broker_failure(broker_no):
#     print(f"Broker {broker_no}failed off bro")
#     lino = json.loads(cli.get('leadership'))
#     active_brokers = json.loads(cli.get('active_brokers'))
#     active_brokers.remove(broker_no) 
#     for topic in lino:
#         num_part = lino[topic][0]
#         for i in range(1,num_part+1):
#             if(lino[topic][i]==broker_no):
#                 lino[topic][i]=random.choice(active_brokers)
#     cli.set('leadership',json.dumps(lino))
#     cli.set('active_brokers',json.dumps(active_brokers))
#     notify_brokers()

# def keyboardInterruptHandler(signal, frame):
#     on_broker_failure(1)
#     exit(0)
# signal.signal(signal.SIGINT, keyboardInterruptHandler)   
# while True:pass

consumed ={'cars':1}
print(type(consumed['cars']))