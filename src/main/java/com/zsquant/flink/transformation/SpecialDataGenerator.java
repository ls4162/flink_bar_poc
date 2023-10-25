package com.zsquant.flink.transformation;

import com.zsquant.flink.pojo.TheoPriceCalcEntity;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SpecialDataGenerator extends KeyedProcessFunction<String, TheoPriceCalcEntity, TheoPriceCalcEntity> {

    private ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer", Long.class
        );
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(TheoPriceCalcEntity value, Context ctx, Collector<TheoPriceCalcEntity> out) throws Exception {
        out.collect(value);  // Forward the original data

        if (timerState.value() == null) {
            long currentTime = ctx.timerService().currentProcessingTime();
            long nextSpecialDataTime = ((currentTime + 1000) / 1000) * 1000 - 200;
            ctx.timerService().registerProcessingTimeTimer(nextSpecialDataTime);
            timerState.update(nextSpecialDataTime);
        }

//        long currentTime = ctx.timerService().currentProcessingTime();
//        long nextSpecialDataTime = ((currentTime + 1000) / 1000) * 1000 - 200;  // 200ms before the next whole second
//
//        Long timerTimestamp = timerState.value();
//        if (timerTimestamp != null && timerTimestamp < nextSpecialDataTime) {
//            ctx.timerService().deleteProcessingTimeTimer(timerTimestamp);
//            timerState.clear();
//        }
//        if (timerState.value() == null) {
//            ctx.timerService().registerProcessingTimeTimer(nextSpecialDataTime);
//            timerState.update(nextSpecialDataTime);
//        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TheoPriceCalcEntity> out) throws Exception {
        TheoPriceCalcEntity specialData = new TheoPriceCalcEntity();
        specialData.setMockData(true);  // Assume there's a method to mark the data as special
        out.collect(specialData);

        long nextNaturalSecond = (timestamp + 1000);
        ctx.timerService().registerProcessingTimeTimer(nextNaturalSecond);
        timerState.update(nextNaturalSecond);
    }
}

