MERGE INTO topic_partitions AS t
USING ( SELECT topics.cluster_id
             , topics.id AS topic_id
             , ?         AS kafka_id
             , CAST(? AS TIMESTAMP WITH TIME ZONE) AS refreshed_at
        FROM   topics
        WHERE  topics.cluster_id = ?
        AND    topics.kafka_id   = ?
             ) AS n
ON  t.cluster_id     = n.cluster_id
AND t.topic_id       = n.topic_id
AND t.kafka_id       = n.kafka_id

WHEN MATCHED
  THEN
    UPDATE
    SET refreshed_at = n.refreshed_at

WHEN NOT MATCHED
  THEN
    INSERT ( cluster_id
           , topic_id
           , kafka_id
           , discovered_at
           , modified_at
           , refreshed_at
           )
    VALUES ( n.cluster_id
           , n.topic_id
           , n.kafka_id
           , n.refreshed_at
           , n.refreshed_at
           , n.refreshed_at
           )
