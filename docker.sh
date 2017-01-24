


docker rm -f rest-proxy kafka zookeeper  elastic  kibana 

docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
sleep 5
docker run -d --name kafka --net=host -p 9092:9092  confluent/kafka
sleep 10
docker run -d --name rest-proxy --net=host  -p 8082:8082  confluent/rest-proxy


sleep 10

docker exec kafka  /bin/sh -c "/usr/bin/kafka-topics --create --zookeeper  zookeeper:2181 --replication-factor 1 --partitions 1 --topic httptokafka"
docker exec kafka  /bin/sh -c "/usr/bin/kafka-topics --create --zookeeper  zookeeper:2181 --replication-factor 1 --partitions 1 --topic kafkatohttp"

sleep 10

./run.py 10000


docker rm -f rest-proxy kafka zookeeper  elastic kibana 

sleep 5
docker run -d --name elastic  -p 9200:9200 -e "http.host=0.0.0.0" -e "transport.host=127.0.0.1" elasticsearch 
sleep 5
docker run --name kibana -p 5601:5601 --link elastic:elasticsearch -d kibana 

curl -XPUT 'localhost:9200/httptokafka?pretty'
curl -XPUT 'localhost:9200/kafkatohttp?pretty'

sleep 5
curl -s -XPOST localhost:9200/kafkatohttp/bdl/_bulk --data-binary "@kafkatoHttp_output.txt"
curl -s -XPOST localhost:9200/httptokafka/bdl/_bulk --data-binary "@httptoKafka_output.txt"

