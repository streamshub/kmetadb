MERGE INTO acl_resources AS t
USING ( SELECT CAST(? AS INT)     AS cluster_id
             , CAST(? AS VARCHAR) AS resource_type
             , CAST(? AS VARCHAR) AS name
             , CAST(? AS VARCHAR) AS pattern_type
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
             ) AS n
ON  t.cluster_id     = n.cluster_id
AND t.resource_type  = n.resource_type
AND t.name           = n.name
AND t.pattern_type   = n.pattern_type

WHEN MATCHED
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , resource_type
           , name
           , pattern_type
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.resource_type
           , n.name
           , n.pattern_type
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
