MERGE INTO consumer_group_members AS t
USING ( SELECT cg.cluster_id      AS cluster_id
             , cg.id              AS consumer_group_id
             , CAST(? AS VARCHAR) AS member_id
             , CAST(? AS VARCHAR) AS group_instance_id
             , CAST(? AS VARCHAR) AS client_id
             , CAST(? AS VARCHAR) AS host
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   consumer_groups cg
        WHERE  cg.cluster_id = ?
        AND    cg.group_id   = ?
             ) AS n
ON  t.cluster_id        = n.cluster_id
AND t.consumer_group_id = n.consumer_group_id
AND t.member_id         = n.member_id

WHEN MATCHED
    AND t.group_instance_id IS NOT DISTINCT FROM n.group_instance_id
    AND t.client_id         IS NOT DISTINCT FROM n.client_id
    AND t.host              IS NOT DISTINCT FROM n.host
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN MATCHED
  THEN
    UPDATE
    SET group_instance_id       = n.group_instance_id
      , client_id   = n.client_id
      , host    = n.host
      , modified_at  = n.refreshed_at
      , refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , consumer_group_id
           , member_id
           , group_instance_id
           , client_id
           , host
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.consumer_group_id
           , n.member_id
           , n.group_instance_id
           , n.client_id
           , n.host
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
