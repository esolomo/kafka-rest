



#docker rm -f rest-proxy schema-registry kafka zookeeper
#sleep 5
docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
sleep 5
docker run -d --name kafka --net=host   --add-host zookeeper:172.18.0.1 -p 9092:9092  confluent/kafka
sleep 10
docker run -d --name schema-registry   --net=host --add-host zookeeper:172.18.0.1 --add-host kafka:172.18.0.1 confluent/schema-registry
sleep 10
docker run -d --name rest-proxy    --net=host --add-host zookeeper:172.18.0.1 --add-host kafka:172.18.0.1  --add-host schema-registry:172.18.0.1  -p 8082:8082  confluent/rest-proxy

#docker exec -it kafka  "/usr/bin/kafka-topics" -c " --create --zookeeper  zookeeper:2181 --replication-factor 1 --partitions 1 --topic BDLOUT"
#docker exec -it kafka  "/usr/bin/kafka-topics" -c " --create --zookeeper  zookeeper:2181 --replication-factor 1 --partitions 1 --topic BDLINT"
#docker exec -it kafka  "/usr/bin/kafka-topics" -c " --create --zookeeper  zookeeper:2181 --replication-factor 1 --partitions 1 --topic test"
#docker exec -it kafka  "/usr/bin/kafka-topics" -c " --create --zookeeper  zookeeper:2181 --replication-factor 1 --partitions 1 --topic test2"
