package com.zsquant.flink.transformation;

import com.zsquant.flink.pojo.TheoPriceCalcEntity;
import com.zsquant.flink.proto.compiled.Derivative;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class ResampleAndForwardFillFunction extends ProcessWindowFunction<TheoPriceCalcEntity, TheoPriceCalcEntity, String, TimeWindow> {
    private transient MapState<LocalDateTime, TheoPriceCalcEntity> windowState;

    private void initState() {
        MapStateDescriptor<LocalDateTime, TheoPriceCalcEntity> descriptor = new MapStateDescriptor<>(
                "windowState",
                LocalDateTime.class,
                TheoPriceCalcEntity.class
        );
        windowState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void process(String key, Context context, Iterable<TheoPriceCalcEntity> elements, Collector<TheoPriceCalcEntity> out) throws Exception {
        if (windowState == null) {
            initState();
        }
        TheoPriceCalcEntity lastElement = null;
        for (TheoPriceCalcEntity element : elements) {
            if (lastElement == null || element.getProduceTime() > lastElement.getProduceTime()) {
                lastElement = element;
            }
        }
        Instant instant = Instant.ofEpochMilli(context.window().getEnd());
        LocalDateTime windowTime = LocalDateTime.ofInstant(instant, ZoneOffset.ofHours(8));
        if (lastElement != null) {
            lastElement.setWindowTime(windowTime);
            windowState.put(windowTime, lastElement);
            out.collect(lastElement);
        } else {
            LocalDateTime oneSecBefore = windowTime.minusSeconds(1);
            TheoPriceCalcEntity lastEntity = windowState.get(oneSecBefore);
            if (lastEntity != null) {
                windowState.put(windowTime, lastEntity);
                out.collect(lastEntity);
            } else {
                // todo
            }
        }
    }
}
