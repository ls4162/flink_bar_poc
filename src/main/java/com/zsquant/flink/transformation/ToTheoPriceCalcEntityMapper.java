package com.zsquant.flink.transformation;

import com.zsquant.flink.pojo.TheoPriceCalcEntity;
import com.zsquant.flink.proto.compiled.Derivative;
import org.apache.flink.api.common.functions.MapFunction;

public class ToTheoPriceCalcEntityMapper implements MapFunction<Derivative.Event, TheoPriceCalcEntity> {
    @Override
    public TheoPriceCalcEntity map(Derivative.Event value) {
        TheoPriceCalcEntity theoPriceCalcEntity = new TheoPriceCalcEntity();
        theoPriceCalcEntity.setProduceTime(value.getProduceTime());
        theoPriceCalcEntity.setExchange(value.getTheoPrice().getExchange().toStringUtf8());
        theoPriceCalcEntity.setInstrumentId(value.getTheoPrice().getInstrumentId().toStringUtf8());
        theoPriceCalcEntity.setTheoPrice(value.getTheoPrice().getTheoPrice());
        return theoPriceCalcEntity;
    }
}
