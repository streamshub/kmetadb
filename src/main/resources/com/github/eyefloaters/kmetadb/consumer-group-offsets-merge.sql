MERGE INTO consumer_group_offsets AS t
USING ( SELECT cg.cluster_id AS cluster_id
             , cg.id         AS consumer_group_id
             , tp.id         AS topic_partition_id
             , ? AS "offset"
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS offset_timestamp
             , ? AS metadata
             , ? AS leader_epoch
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   consumer_groups  cg
        JOIN   topics           t
        ON     t.cluster_id   = cg.cluster_id
        JOIN   topic_partitions tp
        ON     tp.cluster_id  = t.cluster_id
        AND    tp.topic_id    = t.id
        WHERE  cg.cluster_id  = ?
        AND    cg.group_id    = ?
        AND    t.name         = ?
        AND    tp.kafka_id    = ?
             ) AS n
ON  t.cluster_id         = n.cluster_id
AND t.consumer_group_id  = n.consumer_group_id
AND t.topic_partition_id = n.topic_partition_id

WHEN MATCHED
    AND t."offset"     IS NOT DISTINCT FROM n."offset"
    AND t.offset_timestamp IS NOT DISTINCT FROM n.offset_timestamp
    AND t.metadata     IS NOT DISTINCT FROM n.metadata
    AND t.leader_epoch IS NOT DISTINCT FROM n.leader_epoch
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at
      , velocity = 0

WHEN MATCHED
  THEN
    UPDATE
    SET "offset"       = n."offset"
      , velocity = (n."offset" - t."offset") / extract(EPOCH FROM n.refreshed_at - t.modified_at)
      , offset_timestamp = n.offset_timestamp
      , metadata   = n.metadata
      , leader_epoch    = n.leader_epoch
      , modified_at  = n.refreshed_at
      , refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , consumer_group_id
           , topic_partition_id
           , "offset"
           , offset_timestamp
           , metadata
           , leader_epoch
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.consumer_group_id
           , n.topic_partition_id
           , n."offset"
           , n.offset_timestamp
           , n.metadata
           , n.leader_epoch
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
