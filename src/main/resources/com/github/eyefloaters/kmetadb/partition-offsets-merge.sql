MERGE INTO partition_offsets AS t
USING ( SELECT t0.cluster_id
             , t1.id     AS topic_partition_id
             , ?         AS offset_type
             , ?         AS "offset"
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS "timestamp"
             , ?         AS leader_epoch
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   topics              t0
        JOIN   topic_partitions    t1
        ON     t1.cluster_id     = t0.cluster_id
        AND    t1.topic_id       = t0.id
        WHERE  t0.cluster_id     = ?
        AND    t0.k_topic_id     = ?
        AND    t1.k_partition_id = ?
             ) AS n
ON  t.cluster_id         = n.cluster_id
AND t.topic_partition_id = n.topic_partition_id
AND t.offset_type        = n.offset_type

WHEN MATCHED
    AND t."offset"      IS NOT DISTINCT FROM n."offset"
    AND t."timestamp"   IS NOT DISTINCT FROM n."timestamp"
    AND t.leader_epoch  IS NOT DISTINCT FROM n.leader_epoch
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at
      , velocity = CASE t.offset_type
        WHEN 'LATEST'
          THEN 0
        END

WHEN MATCHED
  THEN
    UPDATE
    SET "offset"     = n."offset"
      , velocity = CASE t.offset_type
        WHEN 'LATEST'
          THEN (n."offset" - t."offset") / extract(EPOCH FROM n.refreshed_at - t.modified_at)
        END
      , "timestamp"  = n."timestamp"
      , leader_epoch = n.leader_epoch
      , modified_at  = n.refreshed_at
      , refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , topic_partition_id
           , offset_type
           , "offset"
           , "timestamp"
           , leader_epoch
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.topic_partition_id
           , n.offset_type
           , n."offset"
           , n."timestamp"
           , n.leader_epoch
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
