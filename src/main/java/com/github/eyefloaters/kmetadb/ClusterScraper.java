package com.github.eyefloaters.kmetadb;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import com.github.eyefloaters.kmetadb.model.Cluster;
import com.github.eyefloaters.kmetadb.model.OffsetMetadataAndTimestamp;
import com.github.eyefloaters.kmetadb.model.TopicPartitionOffset;

@ApplicationScoped
public class ClusterScraper {

    private static final Map<String, OffsetSpec> OFFSET_SPECS = Map.ofEntries(
            Map.entry("EARLIEST", OffsetSpec.earliest()),
            Map.entry("LATEST", OffsetSpec.latest()),
            Map.entry("MAX_TIMESTAMP", OffsetSpec.maxTimestamp()));

    @Inject
    Logger log;

    @Inject
    Map<String, Admin> adminClients;

    @Inject
    Map<String, Consumer<byte[], byte[]>> consumers;

    @Inject
    ApplicationStatus status;

    @PostConstruct
    void startup() {
        adminClients.forEach((clusterName, adminClient) ->
            adminClient.describeCluster()
                .clusterId()
                .toCompletionStage()
                .thenAccept(clusterId -> {
                    MDC.put("cluster.id", clusterId);
                    MDC.put("cluster.name", clusterName);
                    log.info("connected");
                }));
    }

    public Set<String> knownClusterNames() {
        return adminClients.keySet();
    }

    public Cluster scrape(String clusterName) {
        Admin adminClient = adminClients.get(clusterName);
        var clusterResult = adminClient.describeCluster();
        var quorumResult = adminClient.describeMetadataQuorum();

        Cluster cluster = KafkaFuture.allOf(clusterResult.clusterId(), clusterResult.nodes(), clusterResult.controller())
                .toCompletionStage()
                .thenApply(nothing -> newCluster(clusterName, clusterResult))
                .exceptionally(error -> {
                    status.assertRunning();
                    log.warnf("exception describing cluster: %s", error.getMessage());
                    throw error instanceof CompletionException ce ? ce : new CompletionException(error);
                })
                .toCompletableFuture()
                .join();

        MDC.put("cluster.id", cluster.kafkaId());

        return CompletableFuture.completedStage(cluster)
            .thenCompose(c -> addNodeConfigs(adminClient, c))
            .thenCompose(c -> addLogDirs(adminClient, c))
            .thenCompose(c -> addQuorumInfo(quorumResult, c))
            .thenCompose(c -> addTopicListings(adminClient, c))
            .thenCompose(c -> addTopicDescriptions(adminClient, c))
            .thenCompose(c -> addTopicConfigs(adminClient, c))
            .thenCompose(c -> addConsumerGroups(adminClient, c))
            .thenCompose(c -> addAclBindings(adminClient, c))
            .toCompletableFuture()
            .join();
    }

    public Map<String, Map<TopicPartition, OffsetMetadataAndTimestamp>> scrapeGroupOffsets(Cluster cluster) {
        Admin adminClient = adminClients.get(cluster.name());
        Map<String, Map<TopicPartition, OffsetAndMetadata>> groupOffsets = new HashMap<>();
        ListConsumerGroupOffsetsSpec all = new ListConsumerGroupOffsetsSpec();

        Set<String> groupNames = cluster.consumerGroups().keySet();
        var groupOffsetResults = adminClient.listConsumerGroupOffsets(groupNames
                .stream()
                .collect(Collectors.toMap(Function.identity(), gid -> all)));

        groupNames.stream()
            .map(group ->
                groupOffsetResults.partitionsToOffsetAndMetadata(group)
                    .toCompletionStage()
                    .thenApply(offsets -> groupOffsets.put(group, offsets))
                    .exceptionally(error -> {
                        status.assertRunning();
                        log.warnf("exception listing group offsets for group %s: %s",
                                group,
                                error.getMessage());
                        return null;
                    }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .join();

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

        Set<TopicPartition> assignments = new HashSet<>(targets.keySet());
        Instant deadline = Instant.now().plusSeconds(10);

        for (int r = 0; r < rounds; r++) {
            int round = r;
            int attempt = 0;

            while (!assignments.isEmpty() && Instant.now().isBefore(deadline)) {
                status.assertRunning();
                consumer.assign(assignments);

                assignments.forEach(topicPartition ->
                    consumer.seek(topicPartition, targets.get(topicPartition).get(round).offset()));

                var result = consumer.poll(Duration.between(Instant.now(), deadline));

                log.tracef("consumer polled %s record(s) in round %d, attempt %d",
                        result.count(),
                        round,
                        ++attempt);
                List<TopicPartition> missing = new ArrayList<>();

                assignments.forEach(topicPartition -> {
                    var records = result.records(topicPartition);

                    if (records.isEmpty()) {
                        missing.add(topicPartition);
                    } else {
                        var rec = records.get(0);
                        Instant ts = Instant.ofEpochMilli(rec.timestamp());
                        targets.get(topicPartition).get(round).offsetTimestamp(ts);

                        log.tracef("timestamp of offset %d in topic-partition %s-%d is %s",
                                rec.offset(),
                                rec.topic(),
                                rec.partition(),
                                ts);
                    }
                });

                assignments.retainAll(missing);
            }

            if (!assignments.isEmpty()) {
                assignments.forEach(topicPartition ->
                    log.infof("no records returned for offset %d in topic-partition %s-%d",
                            targets.get(topicPartition).get(round).offset(),
                            topicPartition.topic(),
                            topicPartition.partition()));
            }

            // Setup next round's assignments
            assignments = targets.entrySet()
                .stream()
                .filter(e -> e.getValue().size() > round + 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(HashSet::new));
        }

        consumer.unsubscribe();

        Map<String, Map<TopicPartition, OffsetMetadataAndTimestamp>> offsetsWithTimestamp = new HashMap<>();

        for (var group : groupOffsets.entrySet()) {
            for (var partition : group.getValue().entrySet()) {
                Instant offsetTimestamp = Optional.ofNullable(targets.get(partition.getKey()))
                        .orElseGet(Collections::emptyList)
                        .stream()
                        .filter(tpo -> tpo.offset() == partition.getValue().offset())
                        .findFirst()
                        .map(TopicPartitionOffset::offsetTimestamp)
                        .orElse(null);
                offsetsWithTimestamp.computeIfAbsent(group.getKey(), k -> new HashMap<>())
                    .put(partition.getKey(), new OffsetMetadataAndTimestamp(partition.getValue(), offsetTimestamp));
            }
        }

        return offsetsWithTimestamp;
    }

    public Map<Uuid, Map<Integer, Map<String, ListOffsetsResultInfo>>> scrapeTopicOffests(Cluster cluster) {
        Admin adminClient = adminClients.get(cluster.name());
        Map<Uuid, Map<Integer, Map<String, ListOffsetsResultInfo>>> topicOffsets = new HashMap<>();
        Map<String, Uuid> topicNames = new HashMap<>();

        var topicPartitions = cluster.topicDescriptions()
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
                                status.assertRunning();
                                log.warnf("exception fetching %s topic offsets %s{name=%s}: %s",
                                        spec.getKey(),
                                        topicNames.get(partition.topic()),
                                        partition.topic(),
                                        error.getMessage());
                                return null;
                            }));
            })
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .join();

        return topicOffsets;
    }

    Cluster newCluster(String clusterName, DescribeClusterResult clusterResult) {
        String kafkaId = clusterResult.clusterId().toCompletionStage().toCompletableFuture().join();
        Collection<Node> nodes = clusterResult.nodes().toCompletionStage().toCompletableFuture().join();
        int controllerId = clusterResult.controller().toCompletionStage().toCompletableFuture().join().id();

        return new Cluster(clusterName, kafkaId, nodes, controllerId);
    }

    CompletionStage<Cluster> addLogDirs(Admin adminClient, Cluster cluster) {
        return adminClient.describeLogDirs(cluster.nodes().stream().map(Node::id).toList())
            .descriptions()
            .entrySet()
            .stream()
            .map(entry ->
                entry.getValue()
                    .toCompletionStage()
                    .thenAccept(logDirs -> cluster.logDirs().put(entry.getKey(), logDirs))
                    .exceptionally(error -> {
                        status.assertRunning();
                        log.warnf("exception fetching log dirs for node: %d",
                                entry.getKey());
                        return null;
                    }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .thenApply(nothing -> cluster);
    }

    CompletionStage<Cluster> addQuorumInfo(DescribeMetadataQuorumResult quorumResult, Cluster cluster) {
        return quorumResult.quorumInfo()
            .toCompletionStage()
            .thenAccept(cluster::quorum)
            .exceptionally(error -> {
                status.assertRunning();
                if (error.getCause() instanceof UnsupportedVersionException) {
                    log.debugf("describing metadata quorum not supported by broker %s", cluster.name());
                } else {
                    log.warnf("exception describing metadata quorum: %s", error.getMessage());
                }
                return null;
            })
            .thenApply(nothing -> cluster);
    }

    CompletionStage<Cluster> addTopicListings(Admin adminClient, Cluster cluster) {
        return adminClient.listTopics(new ListTopicsOptions().listInternal(true))
            .listings()
            .toCompletionStage()
            .thenApply(topics -> topics.stream().collect(Collectors.toMap(TopicListing::topicId, Function.identity())))
            .thenAccept(cluster.topicListings()::putAll)
            .thenApply(nothing -> cluster);
    }

    CompletionStage<Cluster> addNodeConfigs(Admin adminClient, Cluster cluster) {
        var nodeResources = cluster.nodes()
                .stream()
                .map(node -> Map.entry(
                        new ConfigResource(ConfigResource.Type.BROKER, node.idString()),
                        node.id()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return scrapeConfigs(
                adminClient,
                nodeResources.keySet(),
                (resource, config) -> cluster.nodeConfigs().put(nodeResources.get(resource), config),
                (resource, error) -> {
                    Integer nodeId = nodeResources.get(resource);
                    log.warnf("exception describing node config %d: %s", nodeId, error.getMessage());
                })
            .thenApply(nothing -> cluster);
    }

    CompletionStage<Cluster> addTopicConfigs(Admin adminClient, Cluster cluster) {
        var topicResources = cluster.topicListings()
                .values()
                .stream()
                .map(listing -> Map.entry(
                        new ConfigResource(ConfigResource.Type.TOPIC, listing.name()),
                        listing.topicId()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return scrapeConfigs(
                adminClient,
                topicResources.keySet(),
                (resource, config) -> cluster.topicConfigs().put(topicResources.get(resource), config),
                (resource, error) -> {
                    var topicId = topicResources.get(resource);
                    log.warnf("exception describing topic config %s{name=%s}: %s",
                            topicId,
                            cluster.topicListings().get(topicId).name(),
                            error.getMessage());
                })
            .thenApply(nothing -> cluster);
    }

    CompletionStage<Void> scrapeConfigs(
            Admin adminClient,
            Collection<ConfigResource> resources,
            BiConsumer<ConfigResource, Config> configConsumer,
            BiConsumer<ConfigResource, Throwable> errorConsumer) {

        return adminClient.describeConfigs(resources, new DescribeConfigsOptions()
                .includeDocumentation(true))
            .values()
            .entrySet()
            .stream()
            .map(pending -> pending
                    .getValue()
                    .toCompletionStage()
                    .thenAccept(config -> configConsumer.accept(pending.getKey(), config))
                    .exceptionally(error -> {
                        status.assertRunning();
                        errorConsumer.accept(pending.getKey(), error);
                        return null;
                    }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll());
    }

    CompletionStage<Cluster> addTopicDescriptions(Admin adminClient, Cluster cluster) {
        return adminClient.describeTopics(
                TopicCollection.ofTopicIds(cluster.topicListings().keySet()),
                // Allow flexible timeout of 1s per topic
                new DescribeTopicsOptions().timeoutMs(1000 * cluster.topicListings().size()))
            .topicIdValues()
            .entrySet()
            .stream()
            .map(pending -> pending
                    .getValue()
                    .toCompletionStage()
                    .thenAccept(cluster.topicDescriptions()::add)
                    .exceptionally(error -> {
                        status.assertRunning();
                        log.warnf("exception describing topic %s{name=%s}: %s",
                                pending.getKey(),
                                cluster.topicListings().get(pending.getKey()).name(),
                                error.getMessage());
                        return null;
                    }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .thenApply(nothing -> cluster);
    }

    CompletionStage<Cluster> addConsumerGroups(Admin adminClient, Cluster cluster) {
        var listGroupsResult = adminClient.listConsumerGroups();

        return listGroupsResult.errors()
            .toCompletionStage()
            .thenAccept(errors ->
                errors.forEach(error ->
                    log.warnf("exception listing consumer group: %s", error.getMessage())))
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
                                    .thenAccept(group ->
                                        cluster.consumerGroups().put(pending.getKey(), group)))
                            .map(CompletionStage::toCompletableFuture)
                            .collect(awaitingAll())))
            .thenApply(nothing -> cluster);
    }

    CompletionStage<Cluster> addAclBindings(Admin adminClient, Cluster cluster) {
        return adminClient.describeAcls(AclBindingFilter.ANY)
            .values()
            .toCompletionStage()
            .thenAccept(cluster.aclBindings()::addAll)
            .exceptionally(error -> {
                status.assertRunning();
                log.warnf("exception describing ACLs: %s", error.getMessage());
                return null;
            })
            .thenApply(nothing -> cluster);
    }

    static <F extends Object> Collector<CompletableFuture<F>, ?, CompletableFuture<Void>> awaitingAll() {
        return Collectors.collectingAndThen(Collectors.toList(), pending ->
            CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)));
    }

}
