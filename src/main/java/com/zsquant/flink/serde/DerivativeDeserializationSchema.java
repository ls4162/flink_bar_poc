package com.zsquant.flink.serde;

import com.zsquant.flink.proto.compiled.Derivative;
import com.zsquant.flink.proto.compiled.SnapshotEntity;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.Serializable;

public class DerivativeDeserializationSchema extends AbstractDeserializationSchema<Derivative.Event> implements Serializable {
    private transient com.google.protobuf.Parser<Derivative.Event> parser;

    public DerivativeDeserializationSchema(com.google.protobuf.Parser<Derivative.Event> parser) {
        this.parser = parser;
    }

    @Override
    public Derivative.Event deserialize(byte[] message) {
        if (parser == null) {
            parser = Derivative.Event.parser();
        }
        try {
            return Derivative.Event.parseFrom(message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }
}
