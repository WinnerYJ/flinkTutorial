./zookeeper-server-start.sh -daemon ./../config/zookeeper.properties
./kafka-server-start.sh -daemon ./../config/server.properties
./kafka-console-producer.sh --broker-list localhost:9092 --topic clicks
./

flink 192.168.237.10