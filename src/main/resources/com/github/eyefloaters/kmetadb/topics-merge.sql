MERGE INTO topics AS t
USING ( SELECT ? AS cluster_id
             , ? AS k_topic_id
             , ? AS name
             , ? AS internal
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
             ) AS n
ON  t.cluster_id     = n.cluster_id
AND t.k_topic_id     = n.k_topic_id

WHEN MATCHED
    AND t.name       = n.name
    AND t.internal   = n.internal
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN MATCHED
  THEN
    UPDATE
    SET name         = n.name
      , internal     = n.internal
      , modified_at  = n.refreshed_at
      , refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , k_topic_id
           , name
           , internal
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.k_topic_id
           , n.name
           , n.internal
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
