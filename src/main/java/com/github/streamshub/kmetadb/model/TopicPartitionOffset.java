package com.github.streamshub.kmetadb.model;

import java.time.Instant;

import org.apache.kafka.common.TopicPartition;

public class TopicPartitionOffset {
    final TopicPartition partition;
    final long offset;
    Instant offsetTimestamp;

    public TopicPartitionOffset(TopicPartition partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public TopicPartition partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public Instant offsetTimestamp() {
        return offsetTimestamp;
    }

    public void offsetTimestamp(Instant timestamp) {
        offsetTimestamp = timestamp;
    }
}
