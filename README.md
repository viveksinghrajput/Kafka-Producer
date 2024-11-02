1. Start Zookeeper Server
   
   bin\windows\zookeeper-server-start.bat config\zookeeper.properties

3. Start Kafka Server / Broker
   
   bin\windows\kafka-server-start.bat config\server.properties 

5. Create topic
   
    bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic NewTopic --partitions 3 --replication-factor 1 

7. list out all topic names
   
    bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list 

9. Describe topics
    
    bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic NewTopic 

11. Produce message
    
    bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic NewTopic 

13. consume message
    
    bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic NewTopic --from-beginning
