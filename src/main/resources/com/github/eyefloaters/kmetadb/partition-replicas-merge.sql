MERGE INTO partition_replicas AS t
USING ( SELECT t0.cluster_id
             , t1.id     AS topic_partition_id
             , t2.id     AS node_id
             , ?         AS leader
             , ?         AS in_sync
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   topics t0
        JOIN   topic_partitions t1
        ON     t1.cluster_id  = t0.cluster_id
        AND    t1.topic_id    = t0.id
        JOIN   nodes t2
        ON     t2.cluster_id  = t0.cluster_id
        WHERE  t0.cluster_id  = ?
        AND    t0.kafka_id    = ?
        AND    t1.kafka_id    = ?
        AND    t2.kafka_id    = ?
             ) AS n
ON  t.cluster_id         = n.cluster_id
AND t.topic_partition_id = n.topic_partition_id
AND t.node_id            = n.node_id

WHEN MATCHED
    AND t.leader    = n.leader
    AND t.in_sync   = n.in_sync
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN MATCHED
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at
      , leader = n.leader
      , in_sync = n.in_sync

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , topic_partition_id
           , node_id
           , leader
           , in_sync
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.topic_partition_id
           , n.node_id
           , n.leader
           , n.in_sync
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
