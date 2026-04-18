CREATE TABLE IF NOT EXISTS crawler_runtime_stats (
    component TEXT PRIMARY KEY,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at DOUBLE PRECISION NOT NULL
);
