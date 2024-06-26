package com.github.streamshub.kmetadb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;

import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import com.github.streamshub.kmetadb.model.Cluster;

@ApplicationScoped
public class DataSync {

    @Inject
    Logger log;

    @Inject
    ClusterScraper scraper;

    @Inject
    DataSource dataSource;

    @Inject
    UserTransaction transaction;

    @Inject
    ManagedExecutor exec;

    @Inject
    ApplicationStatus status;

    void start(@Observes Startup startupEvent /* NOSONAR */) {
        for (String clusterName : scraper.knownClusterNames()) {
            exec.submit(() -> metadataLoop(clusterName));
        }
    }

    void metadataLoop(String clusterName) {
        MDC.put("cluster.name", clusterName);

        while (status.isRunning()) {
            Instant deadline = Instant.now().plusSeconds(30);

            try {
                refreshMetadata(clusterName, deadline);
            } catch (Exception e) {
                log.errorf(e, "Failed to refresh Kafka cluster metadata");
            }

            if (status.isRunning()) {
                long remaining = deadline.getEpochSecond() - Instant.now().getEpochSecond();

                if (remaining > 0) {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(remaining));
                }
            }
        }

        log.infof("Exited run-loop for cluster %s", clusterName);
    }

    void refreshMetadata(String clusterName, Instant deadline) {
        final Instant begin = Instant.now();
        final Timestamp now = Timestamp.from(begin.truncatedTo(ChronoUnit.MICROS));

        Cluster cluster = scraper.scrape(clusterName);

        try {
            transaction.begin();
            refreshCluster(now, cluster);
            refreshNodes(now, cluster);
            refreshTopics(now, cluster);
            refreshTopicPartitions(now, cluster);
            refreshConsumerGroups(now, cluster);
            refreshAclBindings(now, cluster);
            transaction.commit();
        } catch (Exception e) {
            logException(e, "Kafka cluster metadata");
            return;
        }

        if (log.isDebugEnabled()) {
            log.debugf("refreshed cluster metadata in %s\n%s",
                    Duration.between(begin, Instant.now()),
                    cluster.report());
        }

        while (Instant.now().isBefore(deadline)) {
            status.assertRunning();
            final Instant throttle = Instant.now().plusSeconds(2);

            try {
                transaction.begin();
                refreshConsumerGroupOffsets(cluster);
                refreshTopicOffsets(cluster);
                transaction.commit();
            } catch (Exception e) {
                logException(e, "Kafka offsets");
                return;
            }

            long remaining = throttle.toEpochMilli() - Instant.now().toEpochMilli();

            if (remaining > 0) {
                log.tracef("Parking for %d millis", remaining);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(remaining));
                log.trace("Unparked");
            }
        }
    }

    void refreshCluster(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("clusters-merge"))) {
                Instant t0 = Instant.now();
                stmt.setString(1, cluster.kafkaId());
                stmt.setString(2, cluster.name());
                stmt.setTimestamp(3, now);
                logRefresh("clusters", t0, new int[] { stmt.executeUpdate() });
            }

            try (var stmt = connection.prepareStatement("SELECT id FROM clusters WHERE k_cluster_id = ?")) {
                stmt.setString(1, cluster.kafkaId());

                try (var result = stmt.executeQuery()) {
                    if (result.next()) {
                        cluster.id(result.getInt(1));
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to refresh `clusters` table");
        }
    }

    void refreshNodes(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("nodes-merge"))) {
                Instant t0 = Instant.now();

                for (var node : cluster.allNodes()) {
                    int p = 0;
                    stmt.setInt(++p, cluster.id());
                    stmt.setInt(++p, node.id());
                    stmt.setString(++p, node.host());
                    stmt.setInt(++p, node.port());
                    stmt.setString(++p, node.rack());
                    stmt.setBoolean(++p, node.id() == cluster.controllerId());
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

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("node-configs-merge"))) {
                Instant t0 = Instant.now();

                for (var node : cluster.nodes()) {
                    for (var config : cluster.liveNodeConfigs().get(node.id()).entries()) {
                        int p = 0;
                        stmt.setString(++p, config.name());
                        stmt.setString(++p, config.value());
                        stmt.setString(++p, config.source().name());
                        stmt.setBoolean(++p, config.isSensitive());
                        stmt.setBoolean(++p, config.isReadOnly());
                        stmt.setString(++p, config.type().name());
                        stmt.setString(++p, config.documentation());
                        stmt.setTimestamp(++p, now);
                        stmt.setInt(++p, cluster.id());
                        stmt.setInt(++p, node.id());
                        stmt.addBatch();
                    }
                }

                logRefresh("node_configs", t0, stmt.executeBatch());
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to refresh `node_configs` table");
        }
    }

    void refreshTopics(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("topics-merge"))) {
                Instant t0 = Instant.now();

                for (var topic : cluster.topicListings().values()) {
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

            try (var stmt = connection.prepareStatement("""
                    SELECT id
                         , name
                         , refreshed_at
                    FROM   topics
                    WHERE  cluster_id = ?
                    AND    refreshed_at < ?
                    """,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        log.infof("deleted topic: %s (refreshed_at=%s, now=%s)", results.getString(2), results.getTimestamp(3), now);
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `topics` table");
        }

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("topic-configs-merge"))) {
                Instant t0 = Instant.now();

                for (var topic : cluster.topicListings().values()) {
                    for (var config : cluster.topicConfigs().get(topic.topicId()).entries()) {
                        int p = 0;
                        stmt.setString(++p, config.name());
                        stmt.setString(++p, config.value());
                        stmt.setString(++p, config.source().name());
                        stmt.setBoolean(++p, config.isSensitive());
                        stmt.setBoolean(++p, config.isReadOnly());
                        stmt.setString(++p, config.type().name());
                        stmt.setString(++p, config.documentation());
                        stmt.setTimestamp(++p, now);
                        stmt.setInt(++p, cluster.id());
                        stmt.setString(++p, topic.topicId().toString());
                        stmt.addBatch();
                    }
                }

                logRefresh("nodes", t0, stmt.executeBatch());
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to refresh `node_configs` table");
        }
    }

    void refreshTopicPartitions(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("topic-partitions-merge"))) {
                Instant t0 = Instant.now();

                for (var topic : cluster.topicDescriptions()) {
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

            try (var stmt = connection.prepareStatement(sql("partition-replicas-merge"))) {
                Instant t0 = Instant.now();

                for (var topic : cluster.topicDescriptions()) {
                    for (var partition : topic.partitions()) {
                        var topicPartition = new TopicPartition(topic.name(), partition.partition());

                        for (var replica : partition.replicas()) {
                            var logDir = Optional.of(cluster.logDirs())
                                    .map(logDirs -> logDirs.get(replica.id()))
                                    .orElseGet(Collections::emptyMap)
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
    }

    void refreshConsumerGroups(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("consumer-groups-merge"))) {
                Instant t0 = Instant.now();

                for (var group : cluster.consumerGroups().values()) {
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
                        log.infof("deleted consumer group: %s", results.getString(2));
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `consumer_groups` table");
        }

        refreshConsumerGroupMembers(now, cluster);
        refreshConsumerGroupAssignments(now, cluster);
    }

    void refreshConsumerGroupMembers(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("consumer-group-members-merge"))) {
                Instant t0 = Instant.now();

                for (var group : cluster.consumerGroups().values()) {
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
                        log.infof("deleted consumer group member: member_id=%s, client_id=%s, group_instance_id=%s",
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
    }

    void refreshConsumerGroupAssignments(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("consumer-group-member-assignments-merge"))) {
                Instant t0 = Instant.now();

                for (var group : cluster.consumerGroups().values()) {
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
                        log.infof("deleted consumer group member assignment: %s", results.getString(1));
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `consumer_group_member_assignments` table");
        }
    }

    void refreshConsumerGroupOffsets(Cluster cluster) {
        var groupOffsets = scraper.scrapeGroupOffsets(cluster);
        Timestamp offsetsRefreshedAt = Timestamp.from(Instant.now().truncatedTo(ChronoUnit.MICROS));

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("consumer-group-offsets-merge"))) {
                Instant t0 = Instant.now();

                for (var group : groupOffsets.entrySet()) {
                    for (var partition : group.getValue().entrySet()) {
                        int p = 0;
                        OffsetAndMetadata offsetAndMeta = partition.getValue().offset();
                        long committedOffset = offsetAndMeta.offset();
                        Timestamp offsetTimestamp = Optional.ofNullable(partition.getValue().timestamp())
                                .map(Timestamp::from)
                                .orElse(null);

                        stmt.setLong(++p, committedOffset);
                        stmt.setTimestamp(++p, offsetTimestamp);
                        stmt.setString(++p, offsetAndMeta.metadata());
                        stmt.setObject(++p, offsetAndMeta.leaderEpoch().orElse(null), Types.BIGINT);
                        stmt.setTimestamp(++p, offsetsRefreshedAt);
                        stmt.setInt(++p, cluster.id());
                        stmt.setString(++p, group.getKey());
                        stmt.setString(++p, partition.getKey().topic());
                        stmt.setInt(++p, partition.getKey().partition());
                        stmt.addBatch();
                    }
                }

                logRefresh("consumer_group_offsets", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("""
                    SELECT id
                         , refreshed_at
                    FROM   consumer_group_offsets
                    WHERE  cluster_id = ?
                    AND    refreshed_at < ?
                    """,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, offsetsRefreshedAt);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        log.infof("deleted consumer group offset: %s (refreshed_at=%s, now=%s)", results.getString(1), results.getTimestamp(2), offsetsRefreshedAt);
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `consumer_group_offsets` table");
        }
    }

    void refreshAclBindings(Timestamp now, Cluster cluster) {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("acl-resources-merge"))) {
                Instant t0 = Instant.now();

                for (var resource : cluster.aclBindings().stream().map(AclBinding::pattern).distinct().toList()) {
                    int p = 0;
                    stmt.setInt(++p, cluster.id());
                    stmt.setString(++p, resource.resourceType().name());
                    stmt.setString(++p, resource.name());
                    stmt.setString(++p, resource.patternType().name());
                    stmt.setTimestamp(++p, now);
                }

                logRefresh("acl_resources", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("""
                    SELECT id
                         , resource_type
                         , name
                         , pattern_type
                    FROM   acl_resources
                    WHERE  cluster_id = ?
                    AND    refreshed_at < ?
                    """,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        if (log.isInfoEnabled()) {
                            var resource = new ResourcePattern(
                                    ResourceType.valueOf(results.getString(2)),
                                    results.getString(3),
                                    PatternType.valueOf(results.getString(4)));
                            log.debugf("deleted ACL Resource: %s", resource);
                        }
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `acl_resources` table");
        }

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("acl-entries-merge"))) {
                Instant t0 = Instant.now();

                for (var entry : cluster.aclBindings().stream().map(AclBinding::entry).distinct().toList()) {
                    int p = 0;
                    stmt.setInt(++p, cluster.id());
                    stmt.setString(++p, entry.principal());
                    stmt.setString(++p, entry.host());
                    stmt.setString(++p, entry.operation().name());
                    stmt.setString(++p, entry.permissionType().name());
                    stmt.setTimestamp(++p, now);
                }

                logRefresh("acl_entries", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("""
                    SELECT id
                         , principal
                         , host
                         , operation
                         , permission_type
                    FROM   acl_entries
                    WHERE  cluster_id = ?
                    AND    refreshed_at < ?
                    """,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                try (var results = stmt.executeQuery()) {
                    while (results.next()) {
                        if (log.isInfoEnabled()) {
                            var resource = new AccessControlEntry(
                                    results.getString(2),
                                    results.getString(3),
                                    AclOperation.valueOf(results.getString(4)),
                                    AclPermissionType.valueOf(results.getString(5)));
                            log.infof("deleted ACL Entry: %s", resource);
                        }
                        results.deleteRow();
                    }
                }
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `acl_resources` table");
        }

        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.prepareStatement(sql("acl-bindings-merge"))) {
                Instant t0 = Instant.now();

                for (var binding : cluster.aclBindings()) {
                    int p = 0;
                    stmt.setTimestamp(++p, now);
                    stmt.setInt(++p, cluster.id());
                    stmt.setString(++p, binding.pattern().resourceType().name());
                    stmt.setString(++p, binding.pattern().name());
                    stmt.setString(++p, binding.pattern().patternType().name());
                    stmt.setString(++p, binding.entry().principal());
                    stmt.setString(++p, binding.entry().host());
                    stmt.setString(++p, binding.entry().operation().name());
                    stmt.setString(++p, binding.entry().permissionType().name());
                }

                logRefresh("acl_bindings", t0, stmt.executeBatch());
            }

            try (var stmt = connection.prepareStatement("""
                    DELETE
                    FROM   acl_bindings
                    WHERE  cluster_id = ?
                    AND    refreshed_at < ?
                    """)) {
                stmt.setInt(1, cluster.id());
                stmt.setTimestamp(2, now);

                int deleteCount = stmt.executeUpdate();
                log.infof("deleted %d ACL Bindings", deleteCount);
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to insert to `acl_bindings` table");
        }
    }

    void refreshTopicOffsets(Cluster cluster) {
        var topicOffsets = scraper.scrapeTopicOffests(cluster);
        Timestamp offsetsRefreshedAt = Timestamp.from(Instant.now().truncatedTo(ChronoUnit.MICROS));

        try (var connection = dataSource.getConnection()) {
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
                            stmt.setTimestamp(++p, offsetsRefreshedAt);
                            stmt.setInt(++p, cluster.id());
                            stmt.setString(++p, topic.getKey().toString());
                            stmt.setInt(++p, partition.getKey());
                            stmt.addBatch();
                        }
                    }
                }

                logRefresh("partition_offsets", t0, stmt.executeBatch());
            }
        } catch (SQLException e1) {
            reportSQLException(e1, "Failed to refresh `partition_offsets` table");
        }
    }

    void logRefresh(String table, Instant begin, int[] actualCounts) {
        int total = Arrays.stream(actualCounts)
            .filter(count -> count >= 0)
            .sum();

        if (total != actualCounts.length) {
            int[] multiUpdates = Arrays.stream(actualCounts)
                .filter(count -> count > 1)
                .toArray();

            log.warnf("refreshed %d %s records, but expected to refresh %d (duration %s)",
                    total,
                    table,
                    actualCounts.length,
                    Duration.between(begin, Instant.now()));

            if (multiUpdates.length > 0) {
                log.warnf("batch entry updated more than 1 row in table %s in %d batch entries",
                        table,
                        multiUpdates.length);
            }
        } else {
            log.debugf("refreshed %d %s records in %s",
                    total,
                    table,
                    Duration.between(begin, Instant.now()));
        }
    }

    void logException(Exception thrown, String content) {
        if (status.isRunning()) {
            log.errorf(thrown, "failed to refresh %s", content);

            try {
                transaction.rollback();
            } catch (IllegalStateException | SecurityException | SystemException rollbackThrown) {
                log.error("failed to rollback transaction", rollbackThrown);
            }
        } else {
            log.warn("shutting down mid-transaction, nothing committed");
            try {
                transaction.rollback();
            } catch (Exception rollbackThrown) {
                // Ignore
            }
        }
    }

    void reportSQLException(SQLException sql, String message) {
        status.assertRunning();

        log.error(message, sql);
        var next = sql.getNextException();
        int n = 0;

        while (next != null) {
            log.errorf(next, "(next %d): %s", ++n, message);
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
}
