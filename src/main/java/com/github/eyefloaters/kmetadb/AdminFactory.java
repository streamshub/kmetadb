package com.github.eyefloaters.kmetadb;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
                var client = Admin.create(buildConfig(cluster.getKey()));
                return Map.entry(cluster.getValue(), client);
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    void closeAdmins(@Disposes Map<String, Admin> admins) {
        admins.values().parallelStream().forEach(Admin::close);
    }

    Map<String, Object> buildConfig(String clusterKey) {
        return AdminClientConfig.configNames()
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
                log.debugf("OVERRIDE config %s for cluster %s", configName, clusterKey);
                return removeQuotes(cfg);
            });
    }

    Optional<String> getDefaultConfig(String clusterKey, String configName) {
        if (defaultClusterConfigs.containsKey(configName)) {
            log.debugf("DEFAULT config %s for cluster %s", configName, clusterKey);
            String cfg = defaultClusterConfigs.get(configName).toString();
            return Optional.of(removeQuotes(cfg));
        }

        return Optional.empty();
    }

    String removeQuotes(String cfg) {
        return cfg.replaceAll("(^[\"'])|([\"']$)", "");
    }
}
