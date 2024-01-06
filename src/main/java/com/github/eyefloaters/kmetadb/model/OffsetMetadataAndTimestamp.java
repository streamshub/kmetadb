package com.github.eyefloaters.kmetadb.model;

import java.time.Instant;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

public record OffsetMetadataAndTimestamp(OffsetAndMetadata offset, Instant timestamp) {

}
