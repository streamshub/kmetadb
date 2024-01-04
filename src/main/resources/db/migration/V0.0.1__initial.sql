CREATE TABLE clusters
( id            INT NOT NULL GENERATED ALWAYS AS IDENTITY
, kafka_id      VARCHAR NOT NULL
, name          VARCHAR NOT NULL
, discovered_at TIMESTAMP WITH TIME ZONE NOT NULL
, modified_at   TIMESTAMP WITH TIME ZONE NOT NULL
, refreshed_at  TIMESTAMP WITH TIME ZONE NOT NULL
-- Constraints
, PRIMARY KEY (id)
, CONSTRAINT kafka_cluster_id UNIQUE(kafka_id)
);

CREATE TABLE nodes
( id            INT NOT NULL GENERATED ALWAYS AS IDENTITY
, cluster_id    INT NOT NULL
, kafka_id      INT NOT NULL
, host          VARCHAR
, port          INT
, rack          VARCHAR
, controller    BOOLEAN NOT NULL
, leader        BOOLEAN NOT NULL
, voter         BOOLEAN NOT NULL
, observer      BOOLEAN NOT NULL
, discovered_at TIMESTAMP WITH TIME ZONE NOT NULL
, modified_at   TIMESTAMP WITH TIME ZONE NOT NULL
, refreshed_at  TIMESTAMP WITH TIME ZONE NOT NULL
-- Constraints
, PRIMARY KEY(id)
, CONSTRAINT fk_node_cluster FOREIGN KEY(cluster_id) REFERENCES clusters(id) ON DELETE CASCADE
, CONSTRAINT kafka_node_id UNIQUE(cluster_id, kafka_id)
);

CREATE TABLE topics
( id            INT NOT NULL GENERATED ALWAYS AS IDENTITY
, cluster_id    INT NOT NULL
, kafka_id      VARCHAR NOT NULL
, name          VARCHAR NOT NULL
, internal      BOOLEAN NOT NULL
, discovered_at TIMESTAMP WITH TIME ZONE NOT NULL
, modified_at   TIMESTAMP WITH TIME ZONE NOT NULL
, refreshed_at  TIMESTAMP WITH TIME ZONE NOT NULL
-- Constraints
, PRIMARY KEY(id)
, CONSTRAINT fk_topic_cluster FOREIGN KEY(cluster_id) REFERENCES clusters(id) ON DELETE CASCADE
, CONSTRAINT kafka_topic_id UNIQUE(cluster_id, kafka_id)
);

CREATE TABLE topic_partitions
( id             INT NOT NULL GENERATED ALWAYS AS IDENTITY
, cluster_id     INT NOT NULL
, topic_id       INT NOT NULL
, kafka_id       INT NOT NULL
, discovered_at  TIMESTAMP WITH TIME ZONE NOT NULL
, modified_at    TIMESTAMP WITH TIME ZONE NOT NULL
, refreshed_at   TIMESTAMP WITH TIME ZONE NOT NULL
-- Constraints
, PRIMARY KEY(id)
, CONSTRAINT fk_topic_partition_cluster FOREIGN KEY(cluster_id)     REFERENCES clusters(id) ON DELETE CASCADE
, CONSTRAINT fk_topic_partition_topic   FOREIGN KEY(topic_id)       REFERENCES topics(id)   ON DELETE CASCADE
, CONSTRAINT kafka_topic_partition_id UNIQUE(cluster_id, topic_id, kafka_id)
);

CREATE TABLE partition_offsets
( id                 INT NOT NULL GENERATED ALWAYS AS IDENTITY
, cluster_id         INT NOT NULL
, topic_partition_id INT NOT NULL
, offset_type        VARCHAR NOT NULL
, "offset"           BIGINT
, "timestamp"        TIMESTAMP WITH TIME ZONE
, leader_epoch       BIGINT
, discovered_at      TIMESTAMP WITH TIME ZONE NOT NULL
, modified_at        TIMESTAMP WITH TIME ZONE NOT NULL
, refreshed_at       TIMESTAMP WITH TIME ZONE NOT NULL
-- Constraints
, PRIMARY KEY(id)
, CONSTRAINT fk_partition_offset_cluster FOREIGN KEY(cluster_id) REFERENCES clusters(id) ON DELETE CASCADE
, CONSTRAINT fk_partition_offset_topic_partition FOREIGN KEY(topic_partition_id) REFERENCES topic_partitions(id) ON DELETE CASCADE
, CONSTRAINT kafka_partition_offset_id UNIQUE(cluster_id, topic_partition_id, offset_type)
);

CREATE TABLE partition_replicas
( id                 INT NOT NULL GENERATED ALWAYS AS IDENTITY
, cluster_id         INT NOT NULL
, topic_partition_id INT NOT NULL
, node_id            INT NOT NULL
, leader             BOOLEAN NOT NULL
, in_sync            BOOLEAN NOT NULL
, discovered_at      TIMESTAMP WITH TIME ZONE NOT NULL
, modified_at        TIMESTAMP WITH TIME ZONE NOT NULL
, refreshed_at       TIMESTAMP WITH TIME ZONE NOT NULL
-- Constraints
, PRIMARY KEY(id)
, CONSTRAINT fk_partition_replica_cluster FOREIGN KEY(cluster_id) REFERENCES clusters(id) ON DELETE CASCADE
, CONSTRAINT fk_partition_replica_topic_partition FOREIGN KEY(topic_partition_id) REFERENCES topic_partitions(id) ON DELETE CASCADE
, CONSTRAINT fk_partition_replica_node FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE
, CONSTRAINT kafka_partition_replica_id UNIQUE(cluster_id, topic_partition_id, node_id)
);
