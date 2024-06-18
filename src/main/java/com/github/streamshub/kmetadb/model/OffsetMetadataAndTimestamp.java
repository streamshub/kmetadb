package com.github.streamshub.kmetadb.model;

import java.time.Instant;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

public record OffsetMetadataAndTimestamp(OffsetAndMetadata offset, Instant timestamp) {

}
