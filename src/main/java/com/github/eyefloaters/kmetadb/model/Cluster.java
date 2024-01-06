package com.github.eyefloaters.kmetadb.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;

public class Cluster {

    final String name;
    final String kafkaId;
    final Collection<Node> nodes;
    final int controllerId;

    final Map<Integer, Map<String, LogDirDescription>> logDirs = new HashMap<>();
    final Map<Uuid, TopicListing> topicListings = new HashMap<>();
    final List<TopicDescription> topicDescriptions = new ArrayList<>();
    final Map<String, ConsumerGroupDescription> consumerGroups = new HashMap<>();

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

    public int controllerId() {
        return controllerId;
    }

    public Map<Integer, Map<String, LogDirDescription>> logDirs() {
        return logDirs;
    }

    public Map<Uuid, TopicListing> topicListings() {
        return topicListings;
    }

    public List<TopicDescription> topicDescriptions() {
        return topicDescriptions;
    }

    public Map<String, ConsumerGroupDescription> consumerGroups() {
        return consumerGroups;
    }

    public void quorum(QuorumInfo quorum) {
        this.quorum = quorum;
    }

    private <T> T mapQuorum(Function<QuorumInfo, T> mapFn) {
        return Optional.ofNullable(quorum).map(mapFn).orElse(null);
    }

    public Boolean isLeader(Node node) {
        return mapQuorum(q -> q.leaderId() == node.id());
    }

    public Boolean isVoter(Node node) {
        return mapQuorum(q -> q.voters().stream().anyMatch(r -> r.replicaId() == node.id()));
    }

    public Boolean isObserver(Node node) {
        return mapQuorum(q -> q.observers().stream().anyMatch(r -> r.replicaId() == node.id()));
    }

    public String report() {
        Map<String, Long> counts = new LinkedHashMap<>();

        counts.put("Nodes", Long.valueOf(nodes.size()));
        counts.put("Topics", Long.valueOf(topicListings.size()));

        var partitions = topicDescriptions.stream().flatMap(d -> d.partitions().stream()).toList();
        counts.put("Partitions", Long.valueOf(partitions.size()));
        counts.put("Replicas", partitions.stream().flatMap(p -> p.replicas().stream()).count());

        counts.put("Consumer Groups", Long.valueOf(consumerGroups.size()));
        counts.put("Consumer Group Members", consumerGroups.values().stream().mapToLong(g -> g.members().size()).sum());
        counts.put("Consumer Group Member Assignments", consumerGroups.values().stream()
                .flatMap(g -> g.members().stream()).mapToLong(m -> m.assignment().topicPartitions().size()).sum());

        int labelWidth = counts.keySet().stream().map(String::length).max(Integer::compare).orElse(30);
        String template = "\t%%-%ds: %%9d%n".formatted(labelWidth);
        StringBuilder format = new StringBuilder();
        counts.forEach((k, v) -> format.append(template.formatted(k, v)));
        return format.toString();
    }
}
