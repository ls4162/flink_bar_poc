package com.zsquant.flink;

import com.zsquant.flink.pojo.TheoPriceCalcEntity;
import com.zsquant.flink.proto.compiled.Derivative;
import com.zsquant.flink.proto.compiled.SnapshotEntity;
import com.zsquant.flink.serde.DerivativeDeserializationSchema;
import com.zsquant.flink.serde.SnapshotEntityDeserializationSchema;
import com.zsquant.flink.transformation.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

        DataStream<TheoPriceCalcEntity> theoPriceDataStream = stream
                .filter(event -> event.hasTheoPrice())
                .map(new ToTheoPriceCalcEntityMapper());

//        DataStream<TheoPriceCalcEntity> resampledStream = theoPriceDataStream
//                .keyBy(t -> t.getExchange() + "." + t.getInstrumentId())
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
//                .process(new ResampleAndForwardFillFunction());

//        DataStream<TheoPriceCalcEntity> resampledStream = theoPriceDataStream
//                .keyBy(t -> t.getExchange() + "." + t.getInstrumentId())
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
//                .process(new SourceResampleFunction())
//                .keyBy(t -> t.getExchange() + "." + t.getInstrumentId())
//                .process(new SourceForwardFillFunction());

        DataStream<TheoPriceCalcEntity> resampledStream = theoPriceDataStream
                .keyBy(t -> t.getExchange() + "." + t.getInstrumentId())
                .process(new SpecialDataGenerator())
                .keyBy(t -> t.getExchange() + "." + t.getInstrumentId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new ResampleAndForwardFillWithSpecialDataFunction());

        resampledStream.print();



        env.execute("Derivative Consumer");
    }
}
