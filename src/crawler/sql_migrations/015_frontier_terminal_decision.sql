ALTER TABLE frontier
    ADD COLUMN IF NOT EXISTS terminal_reason TEXT;

ALTER TABLE frontier
    ADD COLUMN IF NOT EXISTS terminalized_at DOUBLE PRECISION;

UPDATE frontier
SET terminal_reason = COALESCE(terminal_reason, last_error, 'legacy_failed'),
    terminalized_at = COALESCE(terminalized_at, next_fetch_at, added_at)
WHERE status = 'failed'
  AND terminal_reason IS NULL;
