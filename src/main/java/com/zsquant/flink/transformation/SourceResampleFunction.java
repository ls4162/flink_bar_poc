package com.zsquant.flink.transformation;

import com.zsquant.flink.pojo.TheoPriceCalcEntity;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class SourceResampleFunction extends ProcessWindowFunction<TheoPriceCalcEntity, TheoPriceCalcEntity, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<TheoPriceCalcEntity> elements, Collector<TheoPriceCalcEntity> out) throws Exception {
        TheoPriceCalcEntity lastElement = null;
        for (TheoPriceCalcEntity element : elements) {
            if (lastElement == null || element.getProduceTime() > lastElement.getProduceTime()) {
                lastElement = element;
            }
        }
        if (lastElement != null) {
            Instant instant = Instant.ofEpochMilli(context.window().getEnd());
            LocalDateTime windowTime = LocalDateTime.ofInstant(instant, ZoneOffset.ofHours(8));
            lastElement.setWindowTime(windowTime);
            out.collect(lastElement);
        } else {
            // todo
        }
    }
}