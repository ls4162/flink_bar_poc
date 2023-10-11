flink-clients for idea local run

proto compile

download compiler https://github.com/protocolbuffers/protobuf/releases

protoc --java_out=src/main/java SnapshotEntity.proto


Kafka

start zookeeper

bin/zookeeper-server-start.sh config/zookeeper.properties

start kafka

bin/kafka-server-start.sh config/server.properties



Mock producer

create topic

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic mock_snapshot_entity

kafka-topics.sh --list --bootstrap-server localhost:9092


console consume message

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mock_snapshot_entity --property print.timestamp=true print.offset=true --isolation-level read_committed

在最新版本的Flink中，FlinkKafkaConsumer 已被弃用，取而代之的是使用特定版本的 Kafka connector，例如 FlinkKafkaConsumer011 对应 Kafka 0.11.x，FlinkKafkaConsumer010 对应 Kafka 0.10.x，等等。

但是，为了支持Flink与Kafka的新版本，Flink引入了FlinkKafkaConsumer的一个新变种，名为FlinkKafkaConsumer<版本号>，例如FlinkKafkaConsumer08、FlinkKafkaConsumer09、FlinkKafkaConsumer10等。

如果你使用的是Kafka的较新版本，例如2.x，你应该使用FlinkKafkaConsumer的最新版本，例如FlinkKafkaConsumerUniversal。

