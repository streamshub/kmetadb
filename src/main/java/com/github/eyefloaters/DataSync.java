package com.github.eyefloaters;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
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

    AtomicBoolean running = new AtomicBoolean(true);

    void start(@Observes Startup startupEvent /* NOSONAR */) {
        exec.submit(() -> {
            try (var connection = dataSource.getConnection()) {
                try (var stmt = connection.createStatement()) {
                    stmt.execute(CREATE_TOPICS);
                }
            } catch (SQLException e1) {
                log.errorf(e1, "Failed to create `topics` table", e1.getMessage());
            }

            adminClient.describeCluster()
                .clusterId()
                .toCompletionStage()
                .thenAccept(clusterId -> log.infof("Connected to cluster: %s", clusterId));

            while (running.get()) {
                adminClient.listTopics()
                    .listings()
                    .toCompletionStage()
                    .thenAccept(topics -> {
                        try (var connection = dataSource.getConnection()) {
                            var now = Timestamp.from(Instant.now());

                            try (var stmt = connection.prepareStatement(UPSERT_TOPIC)) {
                                for (var topic : topics) {
                                    stmt.setString(1, topic.topicId().toString());
                                    stmt.setString(2, topic.name());
                                    stmt.setBoolean(3, topic.isInternal());
                                    stmt.setTimestamp(4, now);
                                    stmt.addBatch();
                                }
                                int[] updated = stmt.executeBatch();
                                log.infof("Added/updated %d topics", updated.length);
                            }

                            try (var stmt = connection.prepareStatement("DELETE FROM topics WHERE updated_at < ?")) {
                                stmt.setTimestamp(1, now);
                                int deleted = stmt.executeUpdate();
                                log.infof("Deleted %d topics", deleted);
                            }
                        } catch (SQLException e1) {
                            log.errorf(e1, "Failed to insert to `topics` table", e1.getMessage());
                            var next = e1.getNextException();

                            while (next != null) {
                                log.errorf(next, "Failed to insert to `topics` table", next.getMessage());
                                next = next.getNextException();
                            }
                        }
                    });

                try {
                    Thread.sleep(10_000);
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

    static final String CREATE_TOPICS = """
        CREATE TABLE topics
        ( id         INT GENERATED ALWAYS AS IDENTITY
        , kafka_id   VARCHAR
        , name       VARCHAR
        , internal   BOOLEAN
        , created_at TIMESTAMP WITH TIME ZONE
        , updated_at TIMESTAMP WITH TIME ZONE
        , CONSTRAINT kafka_topic_id UNIQUE(kafka_id)
        )
        """;

    static final String UPSERT_TOPIC = """
        MERGE INTO topics AS t
        USING ( SELECT ? AS kafka_id
                     , ? AS name
                     , ? AS internal
                     , CAST(? AS TIMESTAMP WITH TIME ZONE) AS updated_at
                     ) AS n
        ON t.kafka_id = n.kafka_id
        WHEN MATCHED THEN UPDATE
          SET name       = n.name
            , internal   = n.internal
            , updated_at = n.updated_at
        WHEN NOT MATCHED THEN
          INSERT ( kafka_id
                 , name
                 , internal
                 , created_at
                 , updated_at
                 )
          VALUES ( n.kafka_id
                 , n.name
                 , n.internal
                 , n.updated_at
                 , n.updated_at
                 )
        """;
}
