UPDATE url_ledger
SET queue_class = CASE
    WHEN status = 'done' THEN 'recrawl'
    WHEN depth <= 2 THEN 'exploration'
    ELSE 'backlog'
END
WHERE queue_class IS DISTINCT FROM CASE
    WHEN status = 'done' THEN 'recrawl'
    WHEN depth <= 2 THEN 'exploration'
    ELSE 'backlog'
END;
