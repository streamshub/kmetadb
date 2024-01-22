MERGE INTO acl_bindings AS t
USING ( SELECT ar.cluster_id
             , ar.resource_id
             , ae.entry_id
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   acl_resources      ar
        JOIN   acl_entries        ae
        ON     ae.cluster_id      = ar.cluster_id
        WHERE  ar.cluster_id      = ?
        AND    ar.resource_type   = ?
        AND    ar.name            = ?
        AND    ar.pattern_type    = ?
        AND    ae.principal       = ?
        AND    ae.host            = ?
        AND    ae.operation       = ?
        AND    ae.permission_type = ?
             ) AS n
ON  t.cluster_id      = n.cluster_id
AND t.resource_id     = n.resource_id
AND t.entry_id        = n.entry_id

WHEN MATCHED
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , resource_id
           , entry_id
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.resource_id
           , n.entry_id
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
