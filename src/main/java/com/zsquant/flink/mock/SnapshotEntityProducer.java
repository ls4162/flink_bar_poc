package com.zsquant.flink.mock;

import com.zsquant.flink.proto.compiled.SnapshotEntity;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class SnapshotEntityProducer {

    private static final String TOPIC = "mock_snapshot_entity";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // 根据你的Kafka配置修改

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (Producer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            long crt = System.currentTimeMillis();
            long startTime = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10);
            Random random = new Random();

            while (true) {
                String code = String.valueOf((char) (random.nextInt(5) + 'a'));
                double price = 100 + (200 - 100) * random.nextDouble();
                long timestamp = startTime += 1000; // 每秒增加

                SnapshotEntity.Event event = SnapshotEntity.Event.newBuilder()
                        .setCode(code)
                        .setPrice(price)
                        .setExchangeTime(timestamp)
                        .build();

                producer.send(new ProducerRecord<>(TOPIC, event.toByteArray()));

                TimeUnit.SECONDS.sleep(1);
            }
        }
    }
}

