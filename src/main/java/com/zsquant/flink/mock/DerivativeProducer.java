package com.zsquant.flink.mock;

import com.zsquant.flink.proto.compiled.Derivative;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.google.protobuf.ByteString;

import java.util.Properties;
import java.util.Random;

public class DerivativeProducer {

    private static final String TOPIC = "mock_derivative";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // 根据你的Kafka配置修改

    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (Producer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            while (true) {
                ByteString exchangeByteString = ByteString.copyFrom("CFFEX".getBytes());
                ByteString instrumentIdByteString = ByteString.copyFrom("MO101".getBytes());
                Derivative.Event event;
                if (random.nextBoolean()) {
                    event = Derivative.Event.newBuilder()
                            .setProduceTime(System.currentTimeMillis())
                            .setTheoPrice(Derivative.TheoPrice.newBuilder()
                                    .setExchange(exchangeByteString)
                                    .setInstrumentId(instrumentIdByteString)
                                    .setTheoPrice(random.nextDouble())
                                    .build())
                            .build();
                } else {
                    event = Derivative.Event.newBuilder()
                            .setProduceTime(System.currentTimeMillis())
                            .setOptionPricingRst(Derivative.OptionPricingRst.newBuilder()
                                    .setExchange(exchangeByteString)
                                    .setInstrumentId(instrumentIdByteString)
                                    .setFairValue(random.nextDouble())
                                    .setDelta(random.nextDouble())
                                    .build())
                            .build();
                }
                producer.send(new ProducerRecord<>(TOPIC, event.toByteArray()));
                Thread.sleep(1000);
            }
        }
    }
}

