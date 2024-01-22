package com.github.eyefloaters.kmetadb.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.QuorumInfo.ReplicaState;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;

public class Cluster {

    final String name;
    final String kafkaId;
    final Collection<Node> nodes;
    final int controllerId;

    final Map<Integer, Config> nodeConfigs = new HashMap<>();
    final Map<Integer, Map<String, LogDirDescription>> logDirs = new HashMap<>();
    final Map<Uuid, TopicListing> topicListings = new HashMap<>();
    final Map<Uuid, Config> topicConfigs = new HashMap<>();
    final List<TopicDescription> topicDescriptions = new ArrayList<>();
    final Map<String, ConsumerGroupDescription> consumerGroups = new HashMap<>();
    final List<AclBinding> aclBindings = new ArrayList<>();

    int id = -1;
    QuorumInfo quorum;

    public Cluster(String name, String kafkaId, Collection<Node> nodes, int controllerId) {
        this.name = name;
        this.kafkaId = kafkaId;
        this.nodes = nodes;
        this.controllerId = controllerId;
    }

    public int id() {
        return id;
    }

    public void id(int id) {
        this.id = id;
    }

    public String name() {
        return name;
    }

    public String kafkaId() {
        return kafkaId;
    }

    public Collection<Node> nodes() {
        return nodes;
    }

    public Map<Integer, Config> nodeConfigs() {
        return nodeConfigs;
    }

    public Map<Integer, Config> liveNodeConfigs() {
        return nodeConfigs.entrySet()
                .stream()
                .filter(e -> nodes.stream().anyMatch(n -> n.id() == e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public int controllerId() {
        return controllerId;
    }

    public Map<Integer, Map<String, LogDirDescription>> logDirs() {
        return logDirs;
    }

    public Map<Uuid, TopicListing> topicListings() {
        return topicListings;
    }

    public Map<Uuid, Config> topicConfigs() {
        return topicConfigs;
    }

    public List<TopicDescription> topicDescriptions() {
        return topicDescriptions;
    }

    public Map<String, ConsumerGroupDescription> consumerGroups() {
        return consumerGroups;
    }

    public List<AclBinding> aclBindings() {
        return aclBindings;
    }

    public void quorum(QuorumInfo quorum) {
        this.quorum = quorum;
    }

    private <T> Optional<T> quorum(Function<QuorumInfo, T> mapFn) {
        return Optional.ofNullable(quorum).map(mapFn);
    }

    public Collection<Node> allNodes() {
        return Stream.concat(nodes.stream(), missingNodes().stream()).toList();
    }

    public Collection<Node> missingNodes() {
        return quorum(q -> Stream.concat(q.voters().stream(), q.observers().stream()))
            .orElseGet(Stream::empty)
            .mapToInt(ReplicaState::replicaId)
            .distinct()
            .filter(nodeId -> nodes.stream().noneMatch(n -> n.id() == nodeId))
            .mapToObj(nodeId -> new Node(nodeId, null, -1))
            .toList();
    }

    public Boolean isLeader(Node node) {
        return quorum(q -> q.leaderId() == node.id()).orElse(null);
    }

    public Boolean isVoter(Node node) {
        return quorum(q -> q.voters().stream().anyMatch(r -> r.replicaId() == node.id())).orElse(null);
    }

    public Boolean isObserver(Node node) {
        return quorum(q -> q.observers().stream().anyMatch(r -> r.replicaId() == node.id())).orElse(null);
    }

    public String report() {
        Map<String, Long> counts = new LinkedHashMap<>();

        counts.put("Nodes", Long.valueOf(nodes.size()));
        counts.put("Nodes (missing)", Long.valueOf(missingNodes().size()));
        counts.put("Node Configs", nodeConfigs.values().stream().mapToLong(c -> c.entries().size()).sum());
        counts.put("Topics", Long.valueOf(topicListings.size()));
        counts.put("Topic Configs", topicConfigs.values().stream().mapToLong(c -> c.entries().size()).sum());

        var partitions = topicDescriptions.stream().flatMap(d -> d.partitions().stream()).toList();
        counts.put("Partitions", Long.valueOf(partitions.size()));
        counts.put("Replicas", partitions.stream().flatMap(p -> p.replicas().stream()).count());

        counts.put("Consumer Groups", Long.valueOf(consumerGroups.size()));
        counts.put("Consumer Group Members", consumerGroups.values().stream().mapToLong(g -> g.members().size()).sum());
        counts.put("Consumer Group Assignments", consumerGroups.values().stream()
                .flatMap(g -> g.members().stream()).mapToLong(m -> m.assignment().topicPartitions().size()).sum());

        counts.put("ACL Resources", aclBindings().stream().map(AclBinding::pattern).distinct().count());
        counts.put("ACL Entries", aclBindings().stream().map(AclBinding::entry).distinct().count());

        int labelWidth = counts.keySet().stream().map(String::length).max(Integer::compare).orElse(30);
        String template = "\t%%-%ds: %%9d%n".formatted(labelWidth);
        StringBuilder format = new StringBuilder();
        counts.forEach((k, v) -> format.append(template.formatted(k, v)));
        return format.toString();
    }
}
