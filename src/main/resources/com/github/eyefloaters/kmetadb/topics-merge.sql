MERGE INTO topics AS t
USING ( SELECT ? AS cluster_id
             , ? AS kafka_id
             , ? AS name
             , ? AS internal
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
             ) AS n
ON  t.cluster_id     = n.cluster_id
AND t.kafka_id       = n.kafka_id

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
           , kafka_id
           , name
           , internal
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.kafka_id
           , n.name
           , n.internal
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
