MERGE INTO nodes AS t
USING ( SELECT CAST(? AS INT)     AS cluster_id
             , CAST(? AS INT)     AS k_node_id
             , CAST(? AS VARCHAR) AS host
             , CAST(? AS INT)     AS port
             , CAST(? AS VARCHAR) AS rack
             , CAST(? AS BOOLEAN) AS controller
             , CAST(? AS BOOLEAN) AS leader
             , CAST(? AS BOOLEAN) AS voter
             , CAST(? AS BOOLEAN) AS observer
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
             ) AS n
ON  t.cluster_id     = n.cluster_id
AND t.k_node_id      = n.k_node_id

WHEN MATCHED
    AND t.host        IS NOT DISTINCT FROM n.host
    AND t.port        IS NOT DISTINCT FROM n.port
    AND t.rack        IS NOT DISTINCT FROM n.rack
    AND t.controller  = n.controller
    AND t.leader      IS NOT DISTINCT FROM n.leader
    AND t.voter       IS NOT DISTINCT FROM n.voter
    AND t.observer    IS NOT DISTINCT FROM n.observer
  THEN
    UPDATE
    SET refreshed_at  = n.refreshed_at

WHEN MATCHED
  THEN
    UPDATE
    SET host          = n.host
      , port          = n.port
      , rack          = n.rack
      , controller    = n.controller
      , leader        = n.leader
      , voter         = n.voter
      , observer      = n.observer
      , modified_at   = n.refreshed_at
      , refreshed_at  = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , k_node_id
           , host
           , port
           , rack
           , controller
           , leader
           , voter
           , observer
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.k_node_id
           , n.host
           , n.port
           , n.rack
           , n.controller
           , n.leader
           , n.voter
           , n.observer
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
