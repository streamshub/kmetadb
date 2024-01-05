MERGE INTO partition_replicas AS t
USING ( SELECT t0.cluster_id
             , t1.id     AS topic_partition_id
             , t2.id     AS node_id
             , ?         AS leader
             , ?         AS in_sync
             , ?         AS size
             , ?         AS offset_lag
             , ?         AS future
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
    AND t.size       IS NOT DISTINCT FROM n.size
    AND t.offset_lag IS NOT DISTINCT FROM n.offset_lag
    AND t.future     IS NOT DISTINCT FROM n.future
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN MATCHED
  THEN
    UPDATE
    SET leader = n.leader
      , in_sync = n.in_sync
      , size = n.size
      , offset_lag = n.offset_lag
      , future = n.future
      , modified_at  = n.refreshed_at
      , refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , topic_partition_id
           , node_id
           , leader
           , in_sync
           , size
           , offset_lag
           , future
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.topic_partition_id
           , n.node_id
           , n.leader
           , n.in_sync
           , n.size
           , n.offset_lag
           , n.future
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
