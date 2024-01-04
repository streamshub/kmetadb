package com.github.eyefloaters.kmetadb;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class AdminFactory {

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
            .map(clusterName -> {
                Map<String, Object> copy = new HashMap<>();

                for (String configName : AdminClientConfig.configNames()) {
                    if (defaultClusterConfigs.containsKey(configName)) {
                        copy.put(configName, defaultClusterConfigs.get(configName));
                    }

                    config.getOptionalValue("kmetadb.kafka." + clusterName.getKey() + '.' + configName, String.class)
                        .ifPresent(cfg -> copy.put(configName, cfg));
                }

                return Map.entry(clusterName.getValue(), Admin.create(copy));
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    void closeAdmins(@Disposes Map<String, Admin> admins) {
        admins.values().parallelStream().forEach(Admin::close);
    }
}
