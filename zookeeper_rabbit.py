import pika
import os

topic_leadership={}


def set_leader(topicName,num_part):
    leader_array = []
    leader_array[0]=num_part
    top=1
    for i in range(1,num_part+1):
        leader_array[i]= top%3+1
        top+=1
    topic_leadership[topicName]=leader_array

def get_leader(topicName,key):
    num_part= topic_leadership[topicName][0]
    pos = hash(key)%(num_part) +1
    return topic_leadership[topicName][pos]

def get_leadership_info():
    return topic_leadership

while True: pass