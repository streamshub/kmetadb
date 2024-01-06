package com.github.eyefloaters.kmetadb;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class AdminFactory {

    @Inject
    Logger log;

    @Inject
    Config config;

    @Inject
    @ConfigProperty(name = "kmetadb.kafka")
    Map<String, String> clusterNames;

    @Inject
    @Identifier("default-kafka-broker")
    Map<String, Object> defaultClusterConfigs;

    @Produces
    Map<String, Admin> getAdmins() {
        return clusterNames.entrySet()
            .stream()
            .map(cluster -> {
                var adminConfig = buildConfig(AdminClientConfig.configNames(), cluster.getKey());
                logConfig("Admin[" + cluster.getKey() + ']', adminConfig);
                var client = Admin.create(adminConfig);
                return Map.entry(unquote(cluster.getValue()), client);
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    void closeAdmins(@Disposes Map<String, Admin> admins) {
        admins.values().parallelStream().forEach(admin -> {
            try {
                admin.close();
            } catch (Exception e) {
                log.warnf("Exception occurred closing admin: %s", e.getMessage());
            }
        });
    }

    @Produces
    Map<String, Consumer<byte[], byte[]>> getConsumers() {
        return clusterNames.entrySet()
            .stream()
            .map(cluster -> {
                Set<String> configNames = ConsumerConfig.configNames().stream()
                        // Do not allow a group Id to be set for this application
                        .filter(Predicate.not(ConsumerConfig.GROUP_ID_CONFIG::equals))
                        .collect(Collectors.toSet());
                var consumerConfig = buildConfig(configNames, cluster.getKey());
                logConfig("Consumer[" + cluster.getKey() + ']', consumerConfig);
                var client = new KafkaConsumer<byte[], byte[]>(consumerConfig);
                return Map.entry(unquote(cluster.getValue()), client);
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    void closeConsumers(@Disposes Map<String, Consumer<byte[], byte[]>> consumers) {
        consumers.values().parallelStream().forEach(consumer -> {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warnf("Exception occurred closing consumer: %s", e.getMessage());
            }
        });
    }

    Map<String, Object> buildConfig(Set<String> configNames, String clusterKey) {
        return configNames
            .stream()
            .map(configName -> getClusterConfig(clusterKey, configName)
                    .or(() -> getDefaultConfig(clusterKey, configName))
                    .map(configValue -> Map.entry(configName, configValue)))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Optional<String> getClusterConfig(String clusterKey, String configName) {
        return config.getOptionalValue("kmetadb.kafka." + clusterKey + '.' + configName, String.class)
            .map(cfg -> {
                log.tracef("OVERRIDE config %s for cluster %s", configName, clusterKey);
                return unquote(cfg);
            });
    }

    Optional<String> getDefaultConfig(String clusterKey, String configName) {
        if (defaultClusterConfigs.containsKey(configName)) {
            log.tracef("DEFAULT config %s for cluster %s", configName, clusterKey);
            String cfg = defaultClusterConfigs.get(configName).toString();
            return Optional.of(unquote(cfg));
        }

        return Optional.empty();
    }

    String unquote(String cfg) {
        return BOUNDARY_QUOTES.matcher(cfg).replaceAll("");
    }

    void logConfig(String clientType, Map<String, Object> config) {
        if (log.isDebugEnabled()) {
            String msg = config.entrySet()
                .stream()
                .map(entry -> "\t%s = %s".formatted(entry.getKey(), entry.getValue()))
                .collect(Collectors.joining("\n", "%s configuration:\n", ""));
            log.debugf(msg, clientType);
        }
    }

    private static final Pattern BOUNDARY_QUOTES = Pattern.compile("(^[\"'])|([\"']$)");
}
