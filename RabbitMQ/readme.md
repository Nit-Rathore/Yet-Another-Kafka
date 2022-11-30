# Implemented Using RabbitMQ , Redis and Python

What does it do ?

## Dynamic Topic Partioning : 
Just like how kafka works 😎
User can choose how many partitions per topic
Data is produced accordingly to the lead broker of a given partition of a given topic.

## Three Fold Active Replication
No compromised made here too 🥵, the given topic is partitioned and  spread across different brokers
Replication makes sure all brokers have all partitions of a given topic

## Dynamic Failure Handling
Supports Pub/Sub even if a single broker is left alive and others are killed 🤝 (Killing = Pressing ctrl+c during runtime) 

## Add as many consumers/producers subscribe or publish to as many topics as you want
Everything just works (Thank God🥳)

### Because messages are stored in key-value , the relative ordering of a particular key type is not lost while logging.


Improvements : Give it some cosmetic improvements , add comments so I dont forget what is what.

## Oh no I forgot to add --from -beginning thing for consumer 😭
