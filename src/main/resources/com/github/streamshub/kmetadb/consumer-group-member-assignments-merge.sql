MERGE INTO consumer_group_member_assignments AS t
USING ( SELECT cg.cluster_id AS cluster_id
             , mbr.id        AS consumer_group_member_id
             , tp.id         AS topic_partition_id
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   consumer_groups         cg
        JOIN   consumer_group_members  mbr
        ON     mbr.cluster_id        = cg.cluster_id
        AND    mbr.consumer_group_id = cg.id
        JOIN   topics                  t
        ON     t.cluster_id          = cg.cluster_id
        JOIN   topic_partitions        tp
        ON     tp.cluster_id         = cg.cluster_id
        AND    tp.topic_id           = t.id
        WHERE  cg.cluster_id         = ?
        AND    cg.group_id           = ?
        AND    t.name                = ?
        AND    tp.k_partition_id     = ?
             ) AS n
ON  t.cluster_id               = n.cluster_id
AND t.consumer_group_member_id = n.consumer_group_member_id
AND t.topic_partition_id       = n.topic_partition_id

WHEN MATCHED
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , consumer_group_member_id
           , topic_partition_id
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.consumer_group_member_id
           , n.topic_partition_id
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
