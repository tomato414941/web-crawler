UPDATE url_ledger
SET queue_class = CASE
    WHEN status = 'done' THEN 'recrawl'
    WHEN archetype IN ('registry_listing', 'redirect_hub') THEN 'backlog'
    ELSE 'backlog'
END
WHERE queue_class IS DISTINCT FROM CASE
    WHEN status = 'done' THEN 'recrawl'
    WHEN archetype IN ('registry_listing', 'redirect_hub') THEN 'backlog'
    ELSE 'backlog'
END;
