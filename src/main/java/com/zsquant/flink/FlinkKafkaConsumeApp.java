package com.zsquant.flink;

import com.zsquant.flink.proto.compiled.SnapshotEntity;
import com.zsquant.flink.serde.SnapshotEntityDeserializationSchema;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.Serializable;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class FlinkKafkaConsumeApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink_consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer<SnapshotEntity.Event> kafkaConsumer = new FlinkKafkaConsumer<>(
                "mock_snapshot_entity",
                new SnapshotEntityDeserializationSchema(SnapshotEntity.Event.parser()),
                properties
        );

        DataStream<SnapshotEntity.Event> stream = env.addSource(kafkaConsumer);

        stream.print();

        env.execute("Flink Kafka Consumer");
    }

}