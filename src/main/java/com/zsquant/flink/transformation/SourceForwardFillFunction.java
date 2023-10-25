package com.zsquant.flink.transformation;


import com.zsquant.flink.pojo.TheoPriceCalcEntity;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SourceForwardFillFunction extends KeyedProcessFunction<String, TheoPriceCalcEntity, TheoPriceCalcEntity> {

    private ValueState<TheoPriceCalcEntity> lastValueState;
    private ValueState<Long> timerState;

    private ValueState<Boolean> hasOutputState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<TheoPriceCalcEntity> descriptor = new ValueStateDescriptor<>(
                "lastValue", TheoPriceCalcEntity.class
        );
        lastValueState = getRuntimeContext().getState(descriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer", Long.class
        );
        timerState = getRuntimeContext().getState(timerDescriptor);

        ValueStateDescriptor<Boolean> hasOutputDescriptor = new ValueStateDescriptor<>(
                "hasOutput", Boolean.class
        );
        hasOutputState = getRuntimeContext().getState(hasOutputDescriptor);
    }

    @Override
    public void processElement(TheoPriceCalcEntity value, Context ctx, Collector<TheoPriceCalcEntity> out) throws Exception {
        lastValueState.update(value);
        out.collect(value);

        long currentTime = ctx.timerService().currentProcessingTime();
        long nextNaturalSecond = ((currentTime + 1000) / 1000) * 1000 ;

        Long timerTimestamp = timerState.value();
        if (timerTimestamp != null && timerTimestamp < nextNaturalSecond) {
            ctx.timerService().deleteProcessingTimeTimer(timerTimestamp);
            timerState.clear();
        }

        if (timerState.value() == null) {
            ctx.timerService().registerProcessingTimeTimer(nextNaturalSecond);
            timerState.update(nextNaturalSecond);
        }
    }

//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TheoPriceCalcEntity> out) throws Exception {
//        timerState.clear();
//        TheoPriceCalcEntity lastValue = lastValueState.value();
//        if (lastValue != null) {
////            lastValue.setWindowTime();
//            out.collect(lastValue);
//        }
////        long nextNaturalSecond = (timestamp + 1000);
//        long nextNaturalSecond = ((timestamp + 1000) / 1000 ) * 1000 + 300;
//        ctx.timerService().registerProcessingTimeTimer(nextNaturalSecond);
//        timerState.update(nextNaturalSecond);
//    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TheoPriceCalcEntity> out) throws Exception {
        timerState.clear();
        Boolean hasOutput = hasOutputState.value();
        if (hasOutput == null || !hasOutput) {
            TheoPriceCalcEntity lastValue = lastValueState.value();
            if (lastValue != null) {
                out.collect(lastValue);
            }
        }
        hasOutputState.clear();
        long nextNaturalSecond = (timestamp + 1000);
        ctx.timerService().registerProcessingTimeTimer(nextNaturalSecond);
        timerState.update(nextNaturalSecond);
    }
}
