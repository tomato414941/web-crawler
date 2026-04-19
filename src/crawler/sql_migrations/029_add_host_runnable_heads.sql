CREATE TABLE IF NOT EXISTS public.host_runnable_heads (
    physical_queue text NOT NULL,
    host text NOT NULL,
    head_url text NOT NULL,
    head_next_fetch_at double precision NOT NULL,
    head_added_at double precision NOT NULL,
    head_priority real NOT NULL,
    runnable_url_count bigint DEFAULT 0 NOT NULL,
    latency_penalty integer DEFAULT 0 NOT NULL,
    runnable_at double precision NOT NULL,
    refreshed_at double precision NOT NULL,
    CONSTRAINT host_runnable_heads_pkey PRIMARY KEY (physical_queue, host),
    CONSTRAINT host_runnable_heads_head_url_fkey
        FOREIGN KEY (head_url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_host_runnable_heads_ready
    ON public.host_runnable_heads(
        physical_queue,
        runnable_at,
        runnable_url_count,
        latency_penalty,
        head_next_fetch_at,
        head_added_at,
        head_priority DESC,
        head_url
    );

