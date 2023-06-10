# RoadTracker

This is a system of vehicle monitoring.

To run, follow the instructions below:

## Config Apache Kafka enviroment:

Ensure that you have Java installed locally (Kafka uses Java). Then:

1. Install the binary release of Kafka [https://www.apache.org/dyn/closer.cgi?path=/kafka/3.4.0/kafka_2.13-3.4.0.tgz](here)
and extract with the following commands:
    ``tar -xzf kafka_<VERSION>.tgz ``
    ``cd kafka_<VERSION> ``

2. Start the Kafka enviroment. For this, you need to
   - Start the ZooKeeper Service:
   ```bin/zookeeper-server-start.sh config/zookeeper.properties```. It's like a "backend" for Kafka Broker.
    
   - In another terminal, start the Kafka Broker Service:
   ```bin/kafka-server-start.sh config/server.properties```

3. Now, you need to create the topic "sensor-data", where the applications are going to write/read events. For this, execute:
```bin/kafka-topics.sh --create --topic sensor-data --bootstrap-server localhost:9092```

> Obs: the local boostrap-server "localhost:9092" is the default server and port of Kafka configurations.


### Testing the Kafka enviroment 
You can test if the local Kafka enviroment is working executing a local consumer and a local producer. For this, you need to:
1. Open a terminal that is your producer
```bin/kafka-console-producer.sh --topic sensor-data --bootstrap-server localhost:9092```

2. Open a terminal that is your consumer
   ```bin/kafka-console-consumer.sh --topic sensor-data --from-beginning --bootstrap-server localhost:9092```

Now, you can write messages in the producer console and see if the consumer console is receiving it.