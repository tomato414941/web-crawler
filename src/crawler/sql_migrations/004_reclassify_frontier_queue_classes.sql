UPDATE frontier
SET queue_class = CASE
    WHEN status = 'done' THEN 'recrawl'
    WHEN discovery_kind = 'seed' THEN 'exploration'
    WHEN discovery_kind = 'same_host' AND depth <= 1 THEN 'exploration'
    WHEN discovery_kind IN ('seed_host', 'external') AND depth <= 2 THEN 'exploration'
    ELSE 'backlog'
END
WHERE queue_class IS DISTINCT FROM CASE
    WHEN status = 'done' THEN 'recrawl'
    WHEN discovery_kind = 'seed' THEN 'exploration'
    WHEN discovery_kind = 'same_host' AND depth <= 1 THEN 'exploration'
    WHEN discovery_kind IN ('seed_host', 'external') AND depth <= 2 THEN 'exploration'
    ELSE 'backlog'
END;
