MERGE INTO acl_entries AS t
USING ( SELECT ? AS cluster_id
             , ? AS principal
             , ? AS host
             , ? AS operation
             , ? AS permission_type
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
             ) AS n
ON  t.cluster_id      = n.cluster_id
AND t.principal       = n.principal
AND t.host            = n.host
AND t.operation       = n.operation
AND t.permission_type = n.permission_type

WHEN MATCHED
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , principal
           , host
           , operation
           , permission_type
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.principal
           , n.host
           , n.operation
           , n.permission_type
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
