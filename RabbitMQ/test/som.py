from redis import Redis
import json,random,signal
from zookeeper_rabbit import notify_brokers
cli = Redis('localhost')

log = json.loads(cli.get('log'))
if(topicName in log.keys()):
    messages=log[topicName]
    messages.append(body)
    log[topicName]=messages
    cli.set("log",json.dumps(log))
else:
    messages=[]
    messages.append(body)
    log[topicName]=messages
    cli.set("log",json.dumps(log))
consumed ={'cars':1}
print(type(consumed['cars']))