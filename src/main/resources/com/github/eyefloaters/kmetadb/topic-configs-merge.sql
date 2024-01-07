MERGE INTO topic_configs AS t
USING ( SELECT topics.cluster_id AS cluster_id
             , topics.id AS topic_id
             , ? AS name
             , ? AS "value"
             , ? AS source
             , ? AS sensitive
             , ? AS read_only
             , ? AS "type"
             , ? AS documentation
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   topics
        WHERE  topics.cluster_id = ?
        AND    topics.kafka_id = ?
             ) AS n
ON  t.cluster_id        = n.cluster_id
AND t.topic_id          = n.topic_id
AND t.name              = n.name

WHEN MATCHED
    AND t."value"       IS NOT DISTINCT FROM n."value"
    AND t.source        = n.source
    AND t.sensitive     = n.sensitive
    AND t.read_only     = n.read_only
    AND t."type"        = n."type"
    AND t.documentation IS NOT DISTINCT FROM n.documentation
  THEN
    UPDATE
    SET refreshed_at    = n.refreshed_at

WHEN MATCHED
  THEN
    UPDATE
    SET "value"         = n."value"
      , source          = n.source
      , sensitive       = n.sensitive
      , read_only       = n.read_only
      , "type"          = n."type"
      , documentation   = n.documentation
      , modified_at     = n.refreshed_at
      , refreshed_at    = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , topic_id
           , name
           , "value"
           , source
           , sensitive
           , read_only
           , "type"
           , documentation
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.topic_id
           , n.name
           , n."value"
           , n.source
           , n.sensitive
           , n.read_only
           , n."type"
           , n.documentation
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
