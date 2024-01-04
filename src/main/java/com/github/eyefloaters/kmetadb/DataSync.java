package com.github.eyefloaters.kmetadb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;

@ApplicationScoped
public class DataSync {

    @Inject
    Logger log;

    @Inject
    ManagedExecutor exec;

    @Inject
    Admin adminClient;

    @Inject
    DataSource dataSource;

    @Inject
    UserTransaction transaction;

    AtomicBoolean running = new AtomicBoolean(true);

    void start(@Observes Startup startupEvent /* NOSONAR */) {
        exec.submit(() -> {
            adminClient.describeCluster()
                .clusterId()
                .toCompletionStage()
                .thenAccept(clusterId -> log.infof("Connected to cluster: %s", clusterId));

            while (running.get()) {
                try {
                    transaction.begin();
                    refreshMetadata();
                    transaction.commit();
                } catch (Exception e) {
                    log.errorf(e, "Failed to refresh Kafka metadata");

                    try {
                        transaction.rollback();
                    } catch (IllegalStateException | SecurityException | SystemException e1) {
                        log.errorf(e1, "Failed to rollback transaction");
                    }
                }

                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    log.warn("Producer thread was interrupted, breaking from loop");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    void stop(@Observes Shutdown shutdownEvent /* NOSONAR */) {
        running.set(false);
    }

    void refreshMetadata() {
        final Instant begin = Instant.now();
        final Timestamp now = Timestamp.from(begin);

        var clusterResult = adminClient.describeCluster();
        var quorumResult = adminClient.describeMetadataQuorum();

        Cluster cluster = KafkaFuture.allOf(
                clusterResult.clusterId(),
                clusterResult.nodes(),
                clusterResult.controller())
            .toCompletionStage()
            .thenApply(nothing -> refreshCluster(now, clusterResult))
            .thenCompose(c -> quorumResult.quorumInfo().toCompletionStage()
                    .thenAccept(c.quorum()::set)
                    .exceptionally(error -> {
                        log.warnf("Exception describing metadata quorum: %s", error.getMessage());
                        return null;
                    })
                    .thenApply(nothing -> c))
            .thenApply(c -> refreshNodes(now, c))
            .toCompletableFuture()
            .join();

        Map<Uuid, String> topicUuids = adminClient.listTopics(new ListTopicsOptions().listInternal(true))
            .listings()
            .toCompletionStage()
            .thenApply(topics -> refreshTopics(now, cluster, topics))
            .toCompletableFuture()
            .join();

        Map<Uuid, TopicDescription> topicDescriptions = new HashMap<>();
        Map<Uuid, Map<Integer, Map<String, ListOffsetsResult.ListOffsetsResultInfo>>> topicOffsets = new HashMap<>();

        var pendingDescriptions = adminClient.describeTopics(TopicCollection.ofTopicIds(topicUuids.keySet()))
            .topicIdValues()
            .entrySet()
            .stream()
            .map(pending -> pending
                    .getValue()
                    .toCompletionStage()
                    .thenAccept(description -> topicDescriptions.put(description.topicId(), description))
                    .exceptionally(error -> {
                        log.warnf("Exception describing topic %s{name=%s} in cluster %s: %s",
                                pending.getKey(),
                                topicUuids.get(pending.getKey()),
                                cluster.id(),
                                error.getMessage());
                        return null;
                    }))
            .map(CompletionStage::toCompletableFuture)
            .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(pendingDescriptions)
            .thenRunAsync(() -> {
                Map<String, Uuid> topicNames = new HashMap<>();

                var topicPartitions = topicDescriptions.values()
                        .stream()
                        .map(d -> {
                            topicNames.put(d.name(), d.topicId());
                            return d;
                        })
                        .flatMap(d -> d.partitions()
                                .stream()
                                .map(p -> new TopicPartition(d.name(), p.partition())))
                        .toList();

                var pendingOffsets = OFFSET_SPECS.entrySet()
                    .stream()
                    .flatMap(spec -> {
                        var request = topicPartitions.stream()
                            .map(p -> Map.entry(p, spec.getValue()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                        var result = adminClient.listOffsets(request);

                        return topicPartitions.stream()
                            .map(partition -> {
                                return result.partitionResult(partition)
                                    .toCompletionStage()
                                    .thenAccept(offset ->
                                        topicOffsets.computeIfAbsent(topicNames.get(partition.topic()), k -> new HashMap<>())
                                            .computeIfAbsent(partition.partition(), k -> new HashMap<>())
                                            .put(spec.getKey(), offset))
                                    .exceptionally(error -> {
                                        log.warnf("Exception fetching %s topic offsets %s{name=%s} in cluster %s: %s",
                                                spec.getKey(),
                                                topicNames.get(partition.topic()),
                                                partition.topic(),
                                                cluster.id(),
                                                error.getMessage());
                                        return null;
                                    });
                            });
                    })
                    .map(CompletionStage::toCompletableFuture)
                    .toArray(CompletableFuture[]::new);

                CompletableFuture.allOf(pendingOffsets).join();
            })
            .thenRunAsync(() -> {
                if (topicDescriptions.isEmpty()) {
                    return;
                }

                try (var connection = dataSource.getConnection()) {
                    try (var stmt = connection.prepareStatement(sql("topic-partitions-merge"))) {
                        Instant t0 = Instant.now();

                        for (var topic : topicDescriptions.values()) {
                            for (var partition : topic.partitions()) {
                                int p = 0;
                                stmt.setInt(++p, partition.partition());
                                stmt.setTimestamp(++p, now);
                                stmt.setInt(++p, cluster.id());
                                stmt.setString(++p, topic.topicId().toString());
                                stmt.addBatch();
                            }
                        }

                        logRefresh("topic_partitions", t0, stmt.executeBatch());
                    }

                    try (var stmt = connection.prepareStatement(sql("partition-offsets-merge"))) {
                        Instant t0 = Instant.now();

                        for (var topic : topicOffsets.entrySet()) {
                            for (var partition : topic.getValue().entrySet()) {
                                for (var offset : partition.getValue().entrySet()) {
                                    Long offsetValue = Optional.of(offset.getValue().offset())
                                        .filter(o -> o >= 0)
                                        .orElse(null);

                                    Timestamp timestamp = Optional.of(offset.getValue().timestamp())
                                        .filter(ts -> ts >= 0)
                                        .map(Instant::ofEpochMilli)
                                        .map(Timestamp::from)
                                        .orElse(null);

                                    int p = 0;
                                    stmt.setString(++p, offset.getKey());
                                    stmt.setObject(++p, offsetValue);
                                    stmt.setTimestamp(++p, timestamp);
                                    stmt.setObject(++p, offset.getValue().leaderEpoch().orElse(null));
                                    stmt.setTimestamp(++p, now);
                                    stmt.setInt(++p, cluster.id());
                                    stmt.setString(++p, topic.getKey().toString());
                                    stmt.setInt(++p, partition.getKey());
                                    stmt.addBatch();
                                }
                            }
                        }

                        logRefresh("partition_offsets", t0, stmt.executeBatch());
                    }

                    try (var stmt = connection.prepareStatement(sql("partition-replicas-merge"))) {
                        Instant t0 = Instant.now();

                        for (var topic : topicDescriptions.values()) {
                            for (var partition : topic.partitions()) {
                                for (var replica : partition.replicas()) {
                                    int p = 0;
                                    stmt.setBoolean(++p, Optional.ofNullable(partition.leader())
                                            .map(leader -> replica.id() == leader.id())
                                            .orElse(false));
                                    stmt.setBoolean(++p, partition.isr()
                                            .stream()
                                            .map(Node::id)
                                            .map(Integer.valueOf(replica.id())::equals)
                                            .filter(Boolean.TRUE::equals)
                                            .findFirst()
                                            .orElse(false));
                                    stmt.setTimestamp(++p, now);
                                    stmt.setInt(++p, cluster.id());
                                    stmt.setString(++p, topic.topicId().toString());
                                    stmt.setInt(++p, partition.partition());
                                    stmt.setInt(++p, replica.id());
                                    stmt.addBatch();
                                }
                            }
                        }

                        logRefresh("partition_replicas", t0, stmt.executeBatch());
                    }
                } catch (SQLException e1) {
                    reportSQLException(e1, "Failed to refresh `topic_partitions` or `partition_replicas` table");
                }
            })
            .join();

//        adminClient.describeConfigs(topicUuids.values()
//                .stream()
//                .map(id -> new ConfigResource(ConfigResource.Type.TOPIC, id))
//                .toList());

        log.infof("""
                Refreshed cluster %s metadata in %s
                \tNodes:      %9d
                \tTopics:     %9d
                \tPartitions: %9d
                \tOffsets:    %9d
                \tReplicas:   %9d""",
                cluster.kafkaId(),
                Duration.between(begin, Instant.now()),
                cluster.nodes().size(),
                topicUuids.size(),
                topicDescriptions.values().stream().flatMap(d -> d.partitions().stream()).count(),
                topicOffsets.values().stream().flatMap(o -> o.values().stream()).mapToInt(o -> o.size()).sum(),
                topicDescriptions.values().stream().flatMap(d -> d.partitions().stream()).flatMap(p -> p.replicas().stream()).count());
    }

    Cluster refreshCluster(Timestamp now, DescribeClusterResult clusterResult) {
        String kafkaId = clusterResult.clusterId().toCompletionStage().toCompletableFuture().join();
        Collection<Node> nodes = clusterResult.nodes().toCompletionStage().toCompletableFuture().join();
        int controllerId = clusterResult.controller().toCompletionStage().toCompletableFuture().join().id();
        int clusterId = -1;

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("clusters-merge"))) {
                stmt.setString(1, kafkaId);
                stmt.setString(2, "my-cluster");
                stmt.setTimestamp(3, now);
                stmt.executeUpdate();
                log.debugf("Added/updated cluster %s", kafkaId);
            }

            try (var stmt = connection.prepareStatement("SELECT id FROM clusters WHERE kafka_id = ?")) {
                stmt.setString(1, kafkaId);

                try (var result = stmt.executeQuery()) {
                    if (result.next()) {
                        clusterId = result.getInt(1);
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to refresh `clusters` table");
        }

        return new Cluster(clusterId, kafkaId, nodes, controllerId, new AtomicReference<>());
    }

    Cluster refreshNodes(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("nodes-merge"))) {
                Instant t0 = Instant.now();

                for (var node : cluster.nodes()) {
                    int p = 0;
                    stmt.setInt(++p, cluster.id());
                    stmt.setInt(++p, node.id());
                    stmt.setString(++p, node.host());
                    stmt.setInt(++p, node.port());
                    stmt.setString(++p, node.rack());
                    stmt.setBoolean(++p, node.id() == cluster.controllerId);
                    stmt.setBoolean(++p, cluster.isLeader(node));
                    stmt.setBoolean(++p, cluster.isVoter(node));
                    stmt.setBoolean(++p, cluster.isObserver(node));
                    stmt.setTimestamp(++p, now);
                    stmt.addBatch();
                }

                logRefresh("nodes", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("SELECT id, kafka_id FROM nodes WHERE cluster_id = ? AND refreshed_at < ?",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        log.infof("Node deleted from cluster %s: %d", cluster.kafkaId(), results.getInt(2));
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to refresh `nodes` table");
        }

        return cluster;
    }

    Map<Uuid, String> refreshTopics(Timestamp now, Cluster cluster, Collection<TopicListing> topics) {
        Map<Uuid, String> topicUuids = new LinkedHashMap<>();

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("topics-merge"))) {
                Instant t0 = Instant.now();

                for (var topic : topics) {
                    topicUuids.put(topic.topicId(), topic.name());
                    int p = 0;
                    stmt.setInt(++p, cluster.id());
                    stmt.setString(++p, topic.topicId().toString());
                    stmt.setString(++p, topic.name());
                    stmt.setBoolean(++p, topic.isInternal());
                    stmt.setTimestamp(++p, now);
                    stmt.addBatch();
                }

                logRefresh("topics", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("SELECT id, name FROM topics WHERE cluster_id = ? AND refreshed_at < ?",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        log.infof("Deleted topic: %s", results.getString(2));
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `topics` table");
        }

        return topicUuids;
    }

    void logRefresh(String table, Instant begin, int[] counts) {
        verifyUpdateCounts(table, counts);
        log.debugf("Refreshed %d %s records in %s", counts.length, table, Duration.between(begin, Instant.now()));
    }

    void verifyUpdateCounts(String table, int[] counts) {
        int[] multiUpdates = Arrays.stream(counts)
            .filter(count -> count > 1)
            .toArray();

        if (multiUpdates.length > 0) {
            log.warnf("Batch entry updated more than 1 row in table %s in %d batch entries", table, multiUpdates.length);
        }
    }

    void reportSQLException(SQLException sql, String message) {
        log.errorf(sql, message);
        var next = sql.getNextException();

        while (next != null) {
            log.errorf(next, message);
            next = next.getNextException();
        }
    }

    static String sql(String resourceName) {
        try (var stream = DataSync.class.getResourceAsStream(resourceName + ".sql")) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    record Cluster(int id, String kafkaId, Collection<Node> nodes, int controllerId, AtomicReference<QuorumInfo> quorum) {
        Optional<QuorumInfo> optQuorum() {
            return Optional.ofNullable(quorum.get());
        }

        boolean isLeader(Node node) {
            return optQuorum()
                    .map(q -> q.leaderId() == node.id())
                    .orElse(false);
        }

        boolean isVoter(Node node) {
            return optQuorum()
                    .map(q -> q.voters().stream().anyMatch(r -> r.replicaId() == node.id()))
                    .orElse(false);            }

        boolean isObserver(Node node) {
            return optQuorum()
                    .map(q -> q.observers().stream().anyMatch(r -> r.replicaId() == node.id()))
                    .orElse(false);            }
    }

    private static final Map<String, OffsetSpec> OFFSET_SPECS = Map.ofEntries(
            Map.entry("EARLIEST", OffsetSpec.earliest()),
            Map.entry("LATEST", OffsetSpec.latest()),
            Map.entry("MAX_TIMESTAMP", OffsetSpec.maxTimestamp()));
}
