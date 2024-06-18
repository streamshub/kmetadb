MERGE INTO consumer_groups AS t
USING ( SELECT nodes.cluster_id   AS cluster_id
             , CAST(? AS VARCHAR) AS group_id
             , nodes.id           AS coordinator_id
             , CAST(? AS BOOLEAN) AS simple
             , CAST(? AS VARCHAR) AS state
             , CAST(? AS VARCHAR) AS partition_assignor
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   nodes
        WHERE  nodes.cluster_id = ?
        AND    nodes.k_node_id  = ?
             ) AS n
ON  t.cluster_id     = n.cluster_id
AND t.group_id       = n.group_id

WHEN MATCHED
    AND t.coordinator_id       = n.coordinator_id
    AND t.simple   = n.simple
    AND t.state    = n.state
    AND t.partition_assignor = n.partition_assignor
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN MATCHED
  THEN
    UPDATE
    SET coordinator_id       = n.coordinator_id
      , simple   = n.simple
      , state    = n.state
      , partition_assignor = n.partition_assignor
      , modified_at  = n.refreshed_at
      , refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , group_id
           , coordinator_id
           , simple
           , state
           , partition_assignor
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.group_id
           , n.coordinator_id
           , n.simple
           , n.state
           , n.partition_assignor
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
