package com.github.streamshub.kmetadb;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ClientFactory {

    @Inject
    Logger log;

    @Inject
    Config config;

    @Inject
    @ConfigProperty(name = "kmetadb.security.trust-certificates", defaultValue = "false")
    boolean trustCertificates;

    @Inject
    @ConfigProperty(name = "kmetadb.kafka")
    Map<String, String> clusterNames;

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
        Map<String, Object> cfg = configNames
            .stream()
            .map(configName -> getClusterConfig(clusterKey, configName)
                    .or(() -> getDefaultConfig(clusterKey, configName))
                    .map(configValue -> Map.entry(configName, configValue)))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (truststoreRequired(cfg)) {
            trustClusterCertificate(cfg);
        }

        return cfg;
    }

    Optional<String> getClusterConfig(String clusterKey, String configName) {
        return config.getOptionalValue("kmetadb.kafka." + clusterKey + '.' + configName, String.class)
            .map(cfg -> {
                log.tracef("OVERRIDE config %s for cluster %s", configName, clusterKey);
                return unquote(cfg);
            });
    }

    Optional<String> getDefaultConfig(String clusterKey, String configName) {
        return config.getOptionalValue("kafka." + configName, String.class)
            .map(cfg -> {
                log.tracef("DEFAULT config %s for cluster %s", configName, clusterKey);
                return unquote(cfg);
            });
    }

    String unquote(String cfg) {
        return BOUNDARY_QUOTES.matcher(cfg).replaceAll("");
    }

    boolean truststoreRequired(Map<String, Object> cfg) {
        var securityProtocol = cfg.getOrDefault(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "");
        var trustStoreMissing = !cfg.containsKey(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG);

        return trustCertificates && trustStoreMissing && securityProtocol.toString().contains("SSL");
    }

    void trustClusterCertificate(Map<String, Object> cfg) {
        TrustManager trustAllCerts = new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return null; // NOSONAR
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) { // NOSONAR
                // all trusted
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) { // NOSONAR
                // all trusted
            }
        };

        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[] { trustAllCerts }, new SecureRandom());
            SSLSocketFactory factory = sc.getSocketFactory();
            String bootstrap = (String) cfg.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
            String[] hostport = bootstrap.split(",")[0].split(":");
            ByteArrayOutputStream certificateOut = new ByteArrayOutputStream();

            try (SSLSocket socket = (SSLSocket) factory.createSocket(hostport[0], Integer.parseInt(hostport[1]))) {
                Certificate[] chain = socket.getSession().getPeerCertificates();
                for (Certificate certificate : chain) {
                    certificateOut.write("-----BEGIN CERTIFICATE-----\n".getBytes(StandardCharsets.UTF_8));
                    certificateOut.write(Base64.getMimeEncoder(80, new byte[] {'\n'}).encode(certificate.getEncoded()));
                    certificateOut.write("\n-----END CERTIFICATE-----\n".getBytes(StandardCharsets.UTF_8));
                }
            }

            cfg.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
                    new String(certificateOut.toByteArray(), StandardCharsets.UTF_8).trim());
            cfg.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            log.warnf("Certificate hosted at %s:%s is automatically trusted", hostport[0], hostport[1]);
        } catch (Exception e) {
            log.infof("Exception setting up trusted certificate: %s", e.getMessage());
        }
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
