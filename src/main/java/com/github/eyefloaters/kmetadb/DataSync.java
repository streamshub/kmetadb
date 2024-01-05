package com.github.eyefloaters.kmetadb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collector;
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
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.jboss.logging.Logger;

@ApplicationScoped
public class DataSync {

    @Inject
    Logger log;

    @Inject
    Map<String, Admin> adminClients;

    @Inject
    Map<String, Consumer<byte[], byte[]>> consumers;

    @Inject
    DataSource dataSource;

    @Inject
    UserTransaction transaction;

    ExecutorService exec = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("metasync-", 1).factory());

    AtomicBoolean running = new AtomicBoolean(true);

    void start(@Observes Startup startupEvent /* NOSONAR */) {
        for (var client : adminClients.entrySet()) {
            exec.submit(() -> metadataLoop(client.getKey(), client.getValue()));
        }
    }

    void stop(@Observes Shutdown shutdownEvent /* NOSONAR */) {
        running.set(false);
        exec.shutdown();
    }

    void metadataLoop(String clusterName, Admin adminClient) {
        adminClient.describeCluster()
            .clusterId()
            .toCompletionStage()
            .thenAcceptAsync(clusterId -> log.infof("Connected to cluster: %s", clusterId), exec);

        while (running.get()) {
            try {
                transaction.begin();
                refreshMetadata(clusterName, adminClient);
                transaction.commit();
            } catch (Exception e) {
                log.errorf(e, "Failed to refresh Kafka metadata");

                try {
                    transaction.rollback();
                } catch (IllegalStateException | SecurityException | SystemException e1) {
                    log.errorf(e1, "Failed to rollback transaction");
                }
            }

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
        }
    }

    void refreshMetadata(String clusterName, Admin adminClient) {
        final Instant begin = Instant.now();
        final Timestamp now = Timestamp.from(begin);

        var clusterResult = adminClient.describeCluster();
        var quorumResult = adminClient.describeMetadataQuorum();

        Cluster cluster = KafkaFuture.allOf(
                clusterResult.clusterId(),
                clusterResult.nodes(),
                clusterResult.controller())
            .toCompletionStage()
            .thenApplyAsync(nothing -> refreshCluster(now, clusterName, clusterResult), exec)
            .thenCompose(c ->
                adminClient.describeLogDirs(c.nodes().stream().map(Node::id).toList())
                    .descriptions()
                    .entrySet()
                    .stream()
                    .map(entry ->
                        entry.getValue()
                            .toCompletionStage()
                            .thenAccept(logDirs -> c.logDirs().put(entry.getKey(), logDirs))
                            .exceptionally(error -> {
                                log.warnf("Exception fetching log dirs for node: %d", entry.getKey());
                                return null;
                            }))
                    .map(CompletionStage::toCompletableFuture)
                    .collect(awaitingAll())
                    .thenApply(nothing -> c))
            .thenCompose(c -> quorumResult.quorumInfo().toCompletionStage()
                    .thenAccept(c.quorum()::set)
                    .exceptionally(error -> {
                        if (error.getCause() instanceof UnsupportedVersionException) {
                            log.debugf("Describing metadata quorum not supported by broker");
                        } else {
                            log.warnf("Exception describing metadata quorum: %s", error.getMessage());
                        }
                        return null;
                    })
                    .thenApply(nothing -> c))
            .thenApplyAsync(c -> refreshNodes(now, c), exec)
            .toCompletableFuture()
            .join();

        Map<Uuid, String> topicUuids = adminClient.listTopics(new ListTopicsOptions().listInternal(true))
            .listings()
            .toCompletionStage()
            .thenApplyAsync(topics -> refreshTopics(now, cluster, topics), exec)
            .toCompletableFuture()
            .join();

        List<TopicDescription> topicDescriptions = new ArrayList<>();
        Map<Uuid, Map<Integer, Map<String, ListOffsetsResult.ListOffsetsResultInfo>>> topicOffsets = new HashMap<>();

        adminClient.describeTopics(
                TopicCollection.ofTopicIds(topicUuids.keySet()),
                // Allow flexible timeout of 1s per topic
                new DescribeTopicsOptions().timeoutMs(1000 * topicUuids.size()))
            .topicIdValues()
            .entrySet()
            .stream()
            .map(pending -> pending
                    .getValue()
                    .toCompletionStage()
                    .thenAccept(topicDescriptions::add)
                    .exceptionally(error -> {
                        log.warnf("Exception describing topic %s{name=%s} in cluster %s: %s",
                                pending.getKey(),
                                topicUuids.get(pending.getKey()),
                                cluster.id(),
                                error.getMessage());
                        return null;
                    }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .thenRunAsync(() -> {
                Map<String, Uuid> topicNames = new HashMap<>();

                var topicPartitions = topicDescriptions
                        .stream()
                        .map(d -> {
                            topicNames.put(d.name(), d.topicId());
                            return d;
                        })
                        .flatMap(d -> d.partitions()
                                .stream()
                                .map(p -> new TopicPartition(d.name(), p.partition())))
                        .toList();

                OFFSET_SPECS.entrySet()
                    .stream()
                    .flatMap(spec -> {
                        var request = topicPartitions.stream()
                            .map(p -> Map.entry(p, spec.getValue()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                        var result = adminClient.listOffsets(request);

                        return topicPartitions.stream()
                            .map(partition ->
                                result.partitionResult(partition)
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
                                    }));
                    })
                    .map(CompletionStage::toCompletableFuture)
                    .collect(awaitingAll())
                    .join();
            }, exec)
            .thenRunAsync(() -> {
                if (topicDescriptions.isEmpty()) {
                    return;
                }

                try (var connection = dataSource.getConnection()) {
                    try (var stmt = connection.prepareStatement(sql("topic-partitions-merge"))) {
                        Instant t0 = Instant.now();

                        for (var topic : topicDescriptions) {
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

                        for (var topic : topicDescriptions) {
                            for (var partition : topic.partitions()) {
                                var topicPartition = new TopicPartition(topic.name(), partition.partition());

                                for (var replica : partition.replicas()) {
                                    var logDir = cluster.logDirs()
                                            .get(replica.id())
                                            .values()
                                            .stream()
                                            .map(dir -> dir.replicaInfos().get(topicPartition))
                                            .filter(Objects::nonNull)
                                            .findFirst();

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
                                    stmt.setObject(++p, logDir.map(ReplicaInfo::size).orElse(null));
                                    stmt.setObject(++p, logDir.map(ReplicaInfo::offsetLag).orElse(null));
                                    stmt.setObject(++p, logDir.map(ReplicaInfo::isFuture).orElse(null));
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
            }, exec)
            .join();

        Map<String, Long> groupCounts = refreshConsumerGroups(now, cluster, adminClient);

//        adminClient.describeConfigs(topicUuids.values()
//                .stream()
//                .map(id -> new ConfigResource(ConfigResource.Type.TOPIC, id))
//                .toList());

        if (log.isDebugEnabled()) {
            Map<String, Long> allCounts = new LinkedHashMap<>();
            allCounts.put("Nodes", (long) cluster.nodes().size());
            allCounts.put("Topics", (long) topicUuids.size());
            allCounts.put("Partitions", topicDescriptions.stream().flatMap(d -> d.partitions().stream()).count());
            allCounts.put("Offsets", topicOffsets.values().stream().flatMap(o -> o.values().stream()).mapToLong(Map::size).sum());
            allCounts.put("Replicas", topicDescriptions.stream().flatMap(d -> d.partitions().stream()).flatMap(p -> p.replicas().stream()).count());
            allCounts.putAll(groupCounts);

            StringBuilder format = new StringBuilder("Refreshed cluster %s metadata in %s\n");
            int labelWidth = allCounts.keySet().stream().map(String::length)
                    .max(Integer::compare)
                    .orElse(30);
            String template = "\t%%-%ds: %%9d%n".formatted(labelWidth);
            allCounts.forEach((k, v) -> format.append(template.formatted(k, v)));
            log.debugf(format.toString(), cluster.kafkaId(), Duration.between(begin, Instant.now()));
        }
    }

    Map<String, ConsumerGroupDescription> describeConsumerGroups(Admin adminClient) {
        var listGroupsResult = adminClient.listConsumerGroups();
        Map<String, ConsumerGroupDescription> describedGroups = new HashMap<>();

        listGroupsResult.errors()
            .toCompletionStage()
            .thenAccept(errors ->
                errors.forEach(error ->
                    log.warnf("Exception listing consumer group: %s", error.getMessage())))
            .thenCompose(nothing ->
                listGroupsResult.valid()
                    .toCompletionStage()
                    .thenApply(listings -> listings.stream().map(ConsumerGroupListing::groupId))
                    .thenApply(groupIds -> adminClient.describeConsumerGroups(groupIds.toList()))
                    .thenCompose(result -> result.describedGroups()
                            .entrySet()
                            .stream()
                            .map(pending ->
                                pending.getValue()
                                    .toCompletionStage()
                                    .thenAccept(group -> describedGroups.put(pending.getKey(), group)))
                            .map(CompletionStage::toCompletableFuture)
                            .collect(awaitingAll())))
            .toCompletableFuture()
            .join();

        return describedGroups;
    }

    Cluster refreshCluster(Timestamp now, String clusterName, DescribeClusterResult clusterResult) {
        String kafkaId = clusterResult.clusterId().toCompletionStage().toCompletableFuture().join();
        Collection<Node> nodes = clusterResult.nodes().toCompletionStage().toCompletableFuture().join();
        int controllerId = clusterResult.controller().toCompletionStage().toCompletableFuture().join().id();
        int clusterId = -1;

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("clusters-merge"))) {
                stmt.setString(1, kafkaId);
                stmt.setString(2, clusterName);
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

        return new Cluster(clusterName, clusterId, kafkaId, nodes, controllerId);
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
                    stmt.setObject(++p, cluster.isLeader(node));
                    stmt.setObject(++p, cluster.isVoter(node));
                    stmt.setObject(++p, cluster.isObserver(node));
                    stmt.setTimestamp(++p, now);
                    stmt.addBatch();
                }

                logRefresh("nodes", t0, stmt.executeBatch());
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

    Map<String, Long> refreshConsumerGroups(Timestamp now, Cluster cluster, Admin adminClient) {
        Map<String, Long> counts = new LinkedHashMap<>();
        var groups = describeConsumerGroups(adminClient);

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("consumer-groups-merge"))) {
                Instant t0 = Instant.now();

                for (var group : groups.values()) {
                    int p = 0;
                    stmt.setString(++p, group.groupId());
                    stmt.setBoolean(++p, group.isSimpleConsumerGroup());
                    stmt.setString(++p, group.state().toString());
                    stmt.setString(++p, group.partitionAssignor());
                    stmt.setTimestamp(++p, now);

                    stmt.setInt(++p, cluster.id());
                    stmt.setInt(++p, group.coordinator().id());
                    stmt.addBatch();
                }

                logRefresh("consumer_groups", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("SELECT id, group_id FROM consumer_groups WHERE cluster_id = ? AND refreshed_at < ?",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        log.infof("Deleted consumer group: %s", results.getString(2));
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `consumer_groups` table");
        }

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("consumer-group-members-merge"))) {
                Instant t0 = Instant.now();

                for (var group : groups.values()) {
                    for (var member : group.members()) {
                        int p = 0;
                        stmt.setString(++p, member.consumerId());
                        stmt.setString(++p, member.groupInstanceId().orElse(null));
                        stmt.setString(++p, member.clientId());
                        stmt.setString(++p, member.host());
                        stmt.setTimestamp(++p, now);
                        stmt.setInt(++p, cluster.id());
                        stmt.setString(++p, group.groupId());
                        stmt.addBatch();
                    }
                }

                logRefresh("consumer_group_members", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("SELECT id, member_id, client_id, group_instance_id FROM consumer_group_members WHERE cluster_id = ? AND refreshed_at < ?",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        log.infof("Deleted consumer group member: member_id=%s, client_id=%s, group_instance_id=%s",
                                results.getString(2),
                                results.getString(3),
                                results.getString(4));
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `consumer_group_members` table");
        }

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("consumer-group-member-assignments-merge"))) {
                Instant t0 = Instant.now();

                for (var group : groups.values()) {
                    for (var member : group.members()) {
                        for (var assignment : member.assignment().topicPartitions()) {
                            int p = 0;
                            stmt.setTimestamp(++p, now);
                            stmt.setInt(++p, cluster.id());
                            stmt.setString(++p, group.groupId());
                            stmt.setString(++p, assignment.topic());
                            stmt.setInt(++p, assignment.partition());
                            stmt.addBatch();
                        }
                    }
                }

                logRefresh("consumer_group_member_assignments", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("SELECT id FROM consumer_group_member_assignments WHERE cluster_id = ? AND refreshed_at < ?",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        log.infof("Deleted consumer group member assignment: %s", results.getString(1));
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `consumer_group_member_assignments` table");
        }

        Map<String, Map<TopicPartition, OffsetAndMetadata>> groupOffsets = new HashMap<>();
        ListConsumerGroupOffsetsSpec all = new ListConsumerGroupOffsetsSpec();
        var groupOffsetResults = adminClient.listConsumerGroupOffsets(groups.keySet()
                .stream()
                .collect(Collectors.toMap(Function.identity(), gid -> all)));

        groups.keySet()
            .stream()
            .map(group ->
                groupOffsetResults.partitionsToOffsetAndMetadata(group)
                    .toCompletionStage()
                    .thenApply(offsets -> groupOffsets.put(group, offsets))
                    .exceptionally(error -> {
                        log.warnf("Exception listing group offsets for group %s in cluster %s: %s",
                                group,
                                cluster.id(),
                                error.getMessage());
                        return null;
                    }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .join();

        class TopicPartitionOffset {
            final TopicPartition partition;
            final long offset;
            Timestamp offsetTimestamp;

            TopicPartitionOffset(TopicPartition partition, long offset) {
                this.partition = partition;
                this.offset = offset;
            }

            TopicPartition partition() {
                return partition;
            }

            long offset() {
                return offset;
            }

            Timestamp offsetTimestamp() {
                return offsetTimestamp;
            }

            void offsetTimestamp(Timestamp timestamp) {
                offsetTimestamp = timestamp;
            }
        }

        Consumer<byte[], byte[]> consumer = consumers.get(cluster.name());
        Set<TopicPartition> allPartitions = groupOffsets.values()
                .stream()
                .map(Map::keySet)
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toSet());

        var beginningOffsets = consumer.beginningOffsets(allPartitions);
        var endOffsets = consumer.endOffsets(allPartitions);

        Map<TopicPartition, List<TopicPartitionOffset>> targets = groupOffsets.values()
                .stream()
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .filter(e -> e.getValue().offset() < endOffsets.get(e.getKey()))
                .filter(e -> e.getValue().offset() >= beginningOffsets.get(e.getKey()))
                .map(e -> new TopicPartitionOffset(e.getKey(), e.getValue().offset()))
                .collect(Collectors.groupingBy(TopicPartitionOffset::partition,
                         Collectors.toCollection(ArrayList::new)));

        int rounds = targets.values().stream().mapToInt(Collection::size).max().orElse(0);

        Set<TopicPartition> assignments = targets.keySet();

        for (int r = 0; r < rounds; r++) {
            consumer.assign(assignments);

            int round = r;

            assignments.forEach(topicPartition ->
                consumer.seek(topicPartition, targets.get(topicPartition).get(round).offset()));

            var result = consumer.poll(Duration.ofSeconds(10));
            log.debugf("Consumer polled %s record(s) in round %d", result.count(), round);

            assignments.forEach(topicPartition -> {
                var records = result.records(topicPartition);

                if (records.isEmpty()) {
                    // format args cast to avoid ambiguous method reference
                    log.infof("No records returned for offset %d in topic-partition %s-%d",
                            (Long) targets.get(topicPartition).get(round).offset(),
                            topicPartition.topic(),
                            (Integer) topicPartition.partition());
                } else {
                    var rec = records.get(0);
                    Timestamp ts = Timestamp.from(Instant.ofEpochMilli(rec.timestamp()));
                    targets.get(topicPartition).get(round).offsetTimestamp(ts);

                    log.debugf("Timestamp of offset %d in topic-partition %s-%d is %s",
                            rec.offset(),
                            rec.topic(),
                            rec.partition(),
                            ts);
                }
            });

            // Setup next round's assignments
            assignments = targets.entrySet()
                .stream()
                .filter(e -> e.getValue().size() > round + 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        }

        consumer.unsubscribe();

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("consumer-group-offsets-merge"))) {
                Instant t0 = Instant.now();

                for (var group : groupOffsets.entrySet()) {
                    for (var partition : group.getValue().entrySet()) {
                        int p = 0;
                        long committedOffset = partition.getValue().offset();
                        Timestamp offsetTimestamp = Optional.ofNullable(targets.get(partition.getKey()))
                            .orElseGet(Collections::emptyList)
                            .stream()
                            .filter(tpo -> tpo.offset == committedOffset)
                            .findFirst()
                            .map(TopicPartitionOffset::offsetTimestamp)
                            .orElse(null);

                        stmt.setLong(++p, committedOffset);
                        stmt.setTimestamp(++p, offsetTimestamp);
                        stmt.setString(++p, partition.getValue().metadata());
                        stmt.setObject(++p, partition.getValue().leaderEpoch().orElse(null), Types.BIGINT);
                        stmt.setTimestamp(++p, now);
                        stmt.setInt(++p, cluster.id());
                        stmt.setString(++p, group.getKey());
                        stmt.setString(++p, partition.getKey().topic());
                        stmt.setInt(++p, partition.getKey().partition());
                        stmt.addBatch();
                    }
                }

                logRefresh("consumer_group_offsets", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("SELECT id FROM consumer_group_offsets WHERE cluster_id = ? AND refreshed_at < ?",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        log.infof("Deleted consumer group offset: %s", results.getString(1));
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `consumer_group_offsets` table");
        }

        counts.put("Consumer Groups", (long) groups.size());
        counts.put("Consumer Group Members", groups.values()
                .stream()
                .mapToLong(g -> g.members().size())
                .sum());
        counts.put("Consumer Group Member Assignments", groups.values()
                .stream()
                .flatMap(g -> g.members().stream())
                .mapToLong(m -> m.assignment().topicPartitions().size())
                .sum());
        counts.put("Consumer Group Offsets", groupOffsets.values()
                .stream()
                .mapToLong(Map::size)
                .sum());

        return counts;
    }

    void logRefresh(String table, Instant begin, int[] actualCounts) {
        int total = Arrays.stream(actualCounts)
            .filter(count -> count >= 0)
            .sum();

        if (total != actualCounts.length) {
            int[] multiUpdates = Arrays.stream(actualCounts)
                .filter(count -> count > 1)
                .toArray();

            log.warnf("Refreshed %d %s records, but expected to refresh %d (duration %s)",
                    total,
                    table,
                    actualCounts.length,
                    Duration.between(begin, Instant.now()));

            if (multiUpdates.length > 0) {
                log.warnf("Batch entry updated more than 1 row in table %s in %d batch entries", table, multiUpdates.length);
            }
        } else {
            log.debugf("Refreshed %d %s records in %s", total, table, Duration.between(begin, Instant.now()));
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

    static <F extends Object> Collector<CompletableFuture<F>, ?, CompletableFuture<Void>> awaitingAll() {
        return Collectors.collectingAndThen(Collectors.toList(), pending ->
            CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)));
    }

    record Cluster(
            String name,
            int id,
            String kafkaId,
            Collection<Node> nodes,
            int controllerId,
            Map<Integer, Map<String, LogDirDescription>> logDirs,
            AtomicReference<QuorumInfo> quorum
    ) {
        public Cluster(String name, int id, String kafkaId, Collection<Node> nodes, int controllerId) {
            this(name, id, kafkaId, nodes, controllerId, new HashMap<>(), new AtomicReference<>());
        }

        private <T> T mapQuorum(Function<QuorumInfo, T> mapFn) {
            return Optional.ofNullable(quorum.get()).map(mapFn).orElse(null);
        }

        Boolean isLeader(Node node) {
            return mapQuorum(q -> q.leaderId() == node.id());
        }

        Boolean isVoter(Node node) {
            return mapQuorum(q -> q.voters().stream().anyMatch(r -> r.replicaId() == node.id()));
        }

        Boolean isObserver(Node node) {
            return mapQuorum(q -> q.observers().stream().anyMatch(r -> r.replicaId() == node.id()));
        }
    }

    private static final Map<String, OffsetSpec> OFFSET_SPECS = Map.ofEntries(
            Map.entry("EARLIEST", OffsetSpec.earliest()),
            Map.entry("LATEST", OffsetSpec.latest()),
            Map.entry("MAX_TIMESTAMP", OffsetSpec.maxTimestamp()));
}
