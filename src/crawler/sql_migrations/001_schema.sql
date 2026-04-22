CREATE TABLE public.url_ledger (
    url text NOT NULL,
    host text NOT NULL,
    discovery_value real DEFAULT 1.0 NOT NULL,
    source_url text,
    added_at double precision NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    current_intent text,
    last_success_at double precision,
    fail_streak integer DEFAULT 0 NOT NULL,
    last_error text,
    terminal_reason text,
    terminalized_at double precision,
    CONSTRAINT url_ledger_pkey PRIMARY KEY (url)
);

CREATE INDEX idx_url_ledger_host
    ON public.url_ledger(host);

CREATE INDEX idx_url_ledger_current_intent
    ON public.url_ledger(current_intent);

CREATE TABLE public.host_ledger (
    host text NOT NULL,
    registrable_domain text,
    first_seen_at double precision NOT NULL,
    last_seen_at double precision NOT NULL,
    last_success_at double precision,
    last_failure_at double precision,
    known_url_count bigint DEFAULT 0 NOT NULL,
    success_count bigint DEFAULT 0 NOT NULL,
    failure_count bigint DEFAULT 0 NOT NULL,
    robots_last_checked_at double precision,
    robots_status text,
    created_at double precision NOT NULL,
    updated_at double precision NOT NULL,
    CONSTRAINT host_ledger_pkey PRIMARY KEY (host)
);

CREATE INDEX idx_host_ledger_registrable_domain
    ON public.host_ledger(registrable_domain);

CREATE INDEX idx_host_ledger_last_seen_at
    ON public.host_ledger(last_seen_at);

CREATE INDEX idx_host_ledger_last_success_at
    ON public.host_ledger(last_success_at);

CREATE INDEX idx_host_ledger_last_failure_at
    ON public.host_ledger(last_failure_at);

CREATE TABLE public.host_state (
    host_key text NOT NULL,
    crawl_delay_seconds double precision DEFAULT 1.0 NOT NULL,
    next_request_at double precision DEFAULT 0 NOT NULL,
    backoff_until double precision DEFAULT 0 NOT NULL,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    robots_checked_at double precision DEFAULT 0 NOT NULL,
    updated_at double precision DEFAULT 0 NOT NULL,
    latency_ewma_ms double precision DEFAULT 0 NOT NULL,
    latency_last_ms double precision DEFAULT 0 NOT NULL,
    latency_observed_at double precision DEFAULT 0 NOT NULL,
    latency_sample_count integer DEFAULT 0 NOT NULL,
    CONSTRAINT host_state_pkey PRIMARY KEY (host_key)
);

CREATE INDEX idx_host_state_next_request_at
    ON public.host_state(next_request_at);

CREATE INDEX idx_host_state_backoff_until
    ON public.host_state(backoff_until);

CREATE TABLE public.pages (
    url_hash text NOT NULL,
    url text NOT NULL,
    host text NOT NULL,
    title text,
    content text,
    status integer,
    content_length integer,
    source_url text,
    outlinks text[],
    crawled_at double precision NOT NULL,
    created_at double precision DEFAULT EXTRACT(epoch FROM now()) NOT NULL,
    CONSTRAINT pages_pkey PRIMARY KEY (url_hash)
);

CREATE INDEX idx_pages_host
    ON public.pages(host);

CREATE INDEX idx_pages_crawled_at
    ON public.pages(crawled_at);

CREATE TABLE public.crawler_runtime_stats (
    component text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at double precision NOT NULL,
    CONSTRAINT crawler_runtime_stats_pkey PRIMARY KEY (component)
);

CREATE TABLE public.scheduler_queue_runnable (
    url text NOT NULL,
    host text NOT NULL,
    scheduler_score real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    CONSTRAINT scheduler_queue_runnable_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_runnable_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_runnable_host
    ON public.scheduler_queue_runnable(host);

CREATE INDEX idx_scheduler_queue_runnable_ready
    ON public.scheduler_queue_runnable(scheduler_score DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_runnable_branch
    ON public.scheduler_queue_runnable(host, branch_key);

CREATE INDEX idx_scheduler_queue_runnable_host_head
    ON public.scheduler_queue_runnable(host, next_fetch_at ASC, scheduler_score DESC, added_at ASC, url ASC);

CREATE TABLE public.scheduler_queue_scheduled (
    url text NOT NULL,
    host text NOT NULL,
    scheduler_score real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    CONSTRAINT scheduler_queue_scheduled_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_scheduled_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_scheduled_host
    ON public.scheduler_queue_scheduled(host);

CREATE INDEX idx_scheduler_queue_scheduled_ready
    ON public.scheduler_queue_scheduled(scheduler_score DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_scheduled_branch
    ON public.scheduler_queue_scheduled(host, branch_key);

CREATE INDEX idx_scheduler_queue_scheduled_host_head
    ON public.scheduler_queue_scheduled(host, next_fetch_at ASC, scheduler_score DESC, added_at ASC, url ASC);

CREATE TABLE public.scheduler_queue_refresh (
    url text NOT NULL,
    host text NOT NULL,
    scheduler_score real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    CONSTRAINT scheduler_queue_refresh_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_refresh_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_refresh_host
    ON public.scheduler_queue_refresh(host);

CREATE INDEX idx_scheduler_queue_refresh_ready
    ON public.scheduler_queue_refresh(scheduler_score DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_refresh_branch
    ON public.scheduler_queue_refresh(host, branch_key);

CREATE INDEX idx_scheduler_queue_refresh_host_head
    ON public.scheduler_queue_refresh(host, next_fetch_at ASC, scheduler_score DESC, added_at ASC, url ASC);

CREATE TABLE public.scheduler_queue_retry_quarantine (
    url text NOT NULL,
    host text NOT NULL,
    physical_queue text NOT NULL,
    scheduler_score real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    quarantined_at double precision DEFAULT EXTRACT(epoch FROM now()) NOT NULL,
    CONSTRAINT scheduler_queue_retry_quarantine_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_retry_quarantine_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_retry_quarantine_host
    ON public.scheduler_queue_retry_quarantine(host);

CREATE INDEX idx_scheduler_queue_retry_quarantine_physical_queue
    ON public.scheduler_queue_retry_quarantine(physical_queue);

CREATE INDEX idx_scheduler_queue_retry_quarantine_branch
    ON public.scheduler_queue_retry_quarantine(host, branch_key);

CREATE INDEX idx_scheduler_queue_retry_quarantine_quarantined_at
    ON public.scheduler_queue_retry_quarantine(quarantined_at);

CREATE TABLE public.active_leases (
    url text NOT NULL,
    host text NOT NULL,
    physical_queue text NOT NULL,
    lease_token text NOT NULL,
    lease_expires_at double precision NOT NULL,
    CONSTRAINT active_leases_pkey PRIMARY KEY (url),
    CONSTRAINT active_leases_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_active_leases_host
    ON public.active_leases(host);

CREATE INDEX idx_active_leases_physical_queue
    ON public.active_leases(physical_queue);

CREATE INDEX idx_active_leases_expiry
    ON public.active_leases(lease_expires_at);

CREATE TABLE public.host_runnable_heads (
    physical_queue text NOT NULL,
    host text NOT NULL,
    head_url text NOT NULL,
    head_next_fetch_at double precision NOT NULL,
    head_added_at double precision NOT NULL,
    head_scheduler_score real NOT NULL,
    runnable_url_count bigint DEFAULT 0 NOT NULL,
    execution_tier integer DEFAULT 1 NOT NULL,
    latency_penalty integer DEFAULT 0 NOT NULL,
    runnable_at double precision NOT NULL,
    refreshed_at double precision NOT NULL,
    CONSTRAINT host_runnable_heads_pkey PRIMARY KEY (physical_queue, host),
    CONSTRAINT host_runnable_heads_head_url_fkey
        FOREIGN KEY (head_url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_host_runnable_heads_ready
    ON public.host_runnable_heads(
        physical_queue,
        execution_tier,
        runnable_at,
        runnable_url_count,
        latency_penalty,
        head_next_fetch_at,
        head_added_at,
        head_scheduler_score DESC,
        head_url
    );

CREATE INDEX idx_host_runnable_heads_head_url
    ON public.host_runnable_heads(head_url);

CREATE TABLE public.host_runnable_head_dirty_hosts (
    physical_queue text NOT NULL,
    host text NOT NULL,
    marked_at double precision NOT NULL,
    CONSTRAINT host_runnable_head_dirty_hosts_pkey PRIMARY KEY (physical_queue, host)
);

CREATE INDEX idx_host_runnable_head_dirty_hosts_queue_marked_at_host
    ON public.host_runnable_head_dirty_hosts(physical_queue, marked_at, host);
