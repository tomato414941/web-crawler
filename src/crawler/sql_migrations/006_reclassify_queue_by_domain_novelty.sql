WITH ranked AS (
    SELECT
        url,
        status,
        archetype,
        depth,
        ROW_NUMBER() OVER (PARTITION BY domain ORDER BY added_at ASC, url ASC) AS domain_rank
    FROM frontier
), reclassified AS (
    SELECT
        url,
        CASE
            WHEN status = 'done' THEN 'recrawl'
            WHEN archetype IN ('registry_listing', 'redirect_hub') THEN 'backlog'
            WHEN domain_rank > 8 THEN 'backlog'
            WHEN depth <= 3 THEN 'exploration'
            ELSE 'backlog'
        END AS queue_class
    FROM ranked
)
UPDATE frontier AS frontier
SET queue_class = reclassified.queue_class
FROM reclassified
WHERE frontier.url = reclassified.url
  AND frontier.queue_class IS DISTINCT FROM reclassified.queue_class;
