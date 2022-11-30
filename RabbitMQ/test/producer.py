from producer_class import producer
import time 
producer.createTopic("cars",3)
producer.createTopic("Bikes",2)
conn,channel = producer.create_connection("cars")
conn,channel = producer.create_connection("Bikes")
producer.publish("cars","volkswagen","Polo",channel)
producer.publish("cars","volkswagen","Tiguan",channel)
producer.publish("cars","tata","Nexon",channel)
producer.publish("cars","tata","Harrier",channel)
producer.publish("cars","toyota","Innova",channel)
producer.publish("cars","toyota","Camry",channel)
producer.publish("cars","audi","Q3",channel)
producer.publish("cars","audi","Q5",channel)
producer.publish("Bikes","Bajaj","CT100",channel)
producer.publish("Bikes","Bajaj","CT110",channel)
producer.publish("Bikes","Yamaha","FZ",channel)
producer.publish("Bikes","TVS","Star City",channel)
producer.publish("Bikes","Kawasaki","Ninja",channel)
producer.publish("Bikes","A","Ninja",channel)

print("Kill something now")
a =10
while(a>0):
    a=a-1
    print(a)
    time.sleep(1)

producer.publish("cars","volkswagen","Polo",channel)
producer.publish("cars","volkswagen","Tiguan",channel)
producer.publish("cars","tata","Nexon",channel)
producer.publish("cars","tata","Harrier",channel)
producer.publish("cars","toyota","Innova",channel)
producer.publish("cars","toyota","Camry",channel)
producer.publish("cars","audi","Q3",channel)
producer.publish("cars","audi","Q5",channel)
producer.publish("Bikes","Bajaj","CT100",channel)
producer.publish("Bikes","Bajaj","CT110",channel)
producer.publish("Bikes","Yamaha","FZ",channel)
producer.publish("Bikes","TVS","Star City",channel)
producer.publish("Bikes","Kawasaki","Ninja",channel)
producer.publish("Bikes","A","Ninja",channel)



producer.close(channel)