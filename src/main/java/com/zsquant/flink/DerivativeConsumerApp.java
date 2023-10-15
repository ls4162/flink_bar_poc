package com.zsquant.flink;

import com.zsquant.flink.proto.compiled.Derivative;
import com.zsquant.flink.proto.compiled.SnapshotEntity;
import com.zsquant.flink.serde.DerivativeDeserializationSchema;
import com.zsquant.flink.serde.SnapshotEntityDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class DerivativeConsumerApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink_consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer<Derivative.Event> kafkaConsumer = new FlinkKafkaConsumer<>(
                "mock_derivative",
                new DerivativeDeserializationSchema(Derivative.Event.parser()),
                properties
        );

        DataStream<Derivative.Event> stream = env.addSource(kafkaConsumer);

        DataStream<Derivative.Event> theoPriceStream = stream
                .filter(event -> event.hasTheoPrice());

        theoPriceStream.print();

        env.execute("Derivative Consumer");
    }
}
