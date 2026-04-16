WITH ranked AS (
    SELECT
        url,
        status,
        archetype,
        depth,
        ROW_NUMBER() OVER (PARTITION BY domain ORDER BY added_at ASC, url ASC) AS domain_rank
    FROM url_ledger
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
UPDATE url_ledger AS url_ledger
SET queue_class = reclassified.queue_class
FROM reclassified
WHERE url_ledger.url = reclassified.url
  AND url_ledger.queue_class IS DISTINCT FROM reclassified.queue_class;
