package com.zsquant.flink.serde;

import com.zsquant.flink.proto.compiled.SnapshotEntity;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.Serializable;

public class SnapshotEntityDeserializationSchema extends AbstractDeserializationSchema<SnapshotEntity.Event> implements Serializable {
    private transient com.google.protobuf.Parser<SnapshotEntity.Event> parser;
    public SnapshotEntityDeserializationSchema( com.google.protobuf.Parser<SnapshotEntity.Event> parser) {
        this.parser = parser;
    }

    @Override
    public SnapshotEntity.Event deserialize(byte[] message) {
        if (parser == null) {
            parser = SnapshotEntity.Event.parser();
        }
        try {
            return SnapshotEntity.Event.parseFrom(message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }
}
