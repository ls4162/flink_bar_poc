package com.zsquant.flink.pojo;

import java.time.LocalDateTime;

public class TheoPriceCalcEntity {
    String exchange;
    String instrumentId;
    Double theoPrice;
    Long produceTime;
    LocalDateTime windowTime;

    Boolean isMockData;

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getInstrumentId() {
        return instrumentId;
    }

    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }

    public Double getTheoPrice() {
        return theoPrice;
    }

    public void setTheoPrice(Double theoPrice) {
        this.theoPrice = theoPrice;
    }

    public Long getProduceTime() {
        return produceTime;
    }

    public void setProduceTime(Long produceTime) {
        this.produceTime = produceTime;
    }

    public LocalDateTime getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(LocalDateTime windowTime) {
        this.windowTime = windowTime;
    }

    public Boolean getMockData() {
        return isMockData;
    }

    public void setMockData(Boolean mockData) {
        isMockData = mockData;
    }

    @Override
    public String toString() {
        return "TheoPriceCalcEntity{" +
                "exchange='" + exchange + '\'' +
                ", instrumentId='" + instrumentId + '\'' +
                ", theoPrice=" + theoPrice +
                ", produceTime=" + produceTime +
                ", windowTime=" + windowTime +
                ", isMockData=" + isMockData +
                '}';
    }
}


