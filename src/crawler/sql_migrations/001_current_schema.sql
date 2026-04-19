CREATE TABLE public.url_ledger (
    url text NOT NULL,
    host text NOT NULL,
    priority real DEFAULT 1.0 NOT NULL,
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

CREATE TABLE public.scheduler_queue_frontline (
    url text NOT NULL,
    host text NOT NULL,
    priority real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    CONSTRAINT scheduler_queue_frontline_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_frontline_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_frontline_host
    ON public.scheduler_queue_frontline(host);

CREATE INDEX idx_scheduler_queue_frontline_ready
    ON public.scheduler_queue_frontline(priority DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_frontline_branch
    ON public.scheduler_queue_frontline(host, branch_key);

CREATE TABLE public.scheduler_queue_deferred (
    url text NOT NULL,
    host text NOT NULL,
    priority real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    CONSTRAINT scheduler_queue_deferred_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_deferred_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_deferred_host
    ON public.scheduler_queue_deferred(host);

CREATE INDEX idx_scheduler_queue_deferred_ready
    ON public.scheduler_queue_deferred(priority DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_deferred_branch
    ON public.scheduler_queue_deferred(host, branch_key);

CREATE TABLE public.scheduler_queue_refresh (
    url text NOT NULL,
    host text NOT NULL,
    priority real DEFAULT 1.0 NOT NULL,
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
    ON public.scheduler_queue_refresh(priority DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_refresh_branch
    ON public.scheduler_queue_refresh(host, branch_key);

CREATE TABLE public.scheduler_queue_retry_quarantine (
    url text NOT NULL,
    host text NOT NULL,
    physical_queue text NOT NULL,
    priority real DEFAULT 1.0 NOT NULL,
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
