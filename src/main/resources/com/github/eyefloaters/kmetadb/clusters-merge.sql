MERGE INTO clusters AS t
USING ( SELECT CAST(? AS VARCHAR) AS k_cluster_id
             , CAST(? AS VARCHAR) AS name
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
             ) AS n
ON t.k_cluster_id = n.k_cluster_id

WHEN MATCHED
    AND t.name       = n.name
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN MATCHED
  THEN
    UPDATE
    SET name         = n.name
      , modified_at  = n.refreshed_at
      , refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( k_cluster_id
           , name
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.k_cluster_id
           , n.name
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
