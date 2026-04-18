CREATE TABLE public.url_ledger (
    url text NOT NULL,
    domain text NOT NULL,
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

CREATE INDEX idx_url_ledger_domain
    ON public.url_ledger(domain);

CREATE INDEX idx_url_ledger_current_intent
    ON public.url_ledger(current_intent);

CREATE TABLE public.domain_state (
    host_key text NOT NULL,
    crawl_delay_seconds double precision DEFAULT 1.0 NOT NULL,
    next_request_at double precision DEFAULT 0 NOT NULL,
    backoff_until double precision DEFAULT 0 NOT NULL,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    robots_checked_at double precision DEFAULT 0 NOT NULL,
    updated_at double precision DEFAULT 0 NOT NULL,
    latency_ewma_ms double precision DEFAULT 0 NOT NULL,
    CONSTRAINT domain_state_pkey PRIMARY KEY (host_key)
);

CREATE INDEX idx_domain_state_next_request_at
    ON public.domain_state(next_request_at);

CREATE INDEX idx_domain_state_backoff_until
    ON public.domain_state(backoff_until);

CREATE TABLE public.pages (
    url_hash text NOT NULL,
    url text NOT NULL,
    domain text NOT NULL,
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

CREATE INDEX idx_pages_domain
    ON public.pages(domain);

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
    domain text NOT NULL,
    priority real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    CONSTRAINT scheduler_queue_frontline_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_frontline_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_frontline_domain
    ON public.scheduler_queue_frontline(domain);

CREATE INDEX idx_scheduler_queue_frontline_ready
    ON public.scheduler_queue_frontline(priority DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_frontline_branch
    ON public.scheduler_queue_frontline(domain, branch_key);

CREATE TABLE public.scheduler_queue_deferred (
    url text NOT NULL,
    domain text NOT NULL,
    priority real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    CONSTRAINT scheduler_queue_deferred_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_deferred_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_deferred_domain
    ON public.scheduler_queue_deferred(domain);

CREATE INDEX idx_scheduler_queue_deferred_ready
    ON public.scheduler_queue_deferred(priority DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_deferred_branch
    ON public.scheduler_queue_deferred(domain, branch_key);

CREATE TABLE public.scheduler_queue_refresh (
    url text NOT NULL,
    domain text NOT NULL,
    priority real DEFAULT 1.0 NOT NULL,
    next_fetch_at double precision DEFAULT 0 NOT NULL,
    added_at double precision DEFAULT 0 NOT NULL,
    branch_key text DEFAULT '/'::text NOT NULL,
    CONSTRAINT scheduler_queue_refresh_pkey PRIMARY KEY (url),
    CONSTRAINT scheduler_queue_refresh_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_scheduler_queue_refresh_domain
    ON public.scheduler_queue_refresh(domain);

CREATE INDEX idx_scheduler_queue_refresh_ready
    ON public.scheduler_queue_refresh(priority DESC, next_fetch_at ASC, added_at ASC);

CREATE INDEX idx_scheduler_queue_refresh_branch
    ON public.scheduler_queue_refresh(domain, branch_key);

CREATE TABLE public.scheduler_queue_retry_quarantine (
    url text NOT NULL,
    domain text NOT NULL,
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

CREATE INDEX idx_scheduler_queue_retry_quarantine_domain
    ON public.scheduler_queue_retry_quarantine(domain);

CREATE INDEX idx_scheduler_queue_retry_quarantine_physical_queue
    ON public.scheduler_queue_retry_quarantine(physical_queue);

CREATE INDEX idx_scheduler_queue_retry_quarantine_branch
    ON public.scheduler_queue_retry_quarantine(domain, branch_key);

CREATE INDEX idx_scheduler_queue_retry_quarantine_quarantined_at
    ON public.scheduler_queue_retry_quarantine(quarantined_at);

CREATE TABLE public.active_leases (
    url text NOT NULL,
    domain text NOT NULL,
    physical_queue text NOT NULL,
    lease_token text NOT NULL,
    lease_expires_at double precision NOT NULL,
    CONSTRAINT active_leases_pkey PRIMARY KEY (url),
    CONSTRAINT active_leases_url_fkey
        FOREIGN KEY (url) REFERENCES public.url_ledger(url) ON DELETE CASCADE
);

CREATE INDEX idx_active_leases_domain
    ON public.active_leases(domain);

CREATE INDEX idx_active_leases_physical_queue
    ON public.active_leases(physical_queue);

CREATE INDEX idx_active_leases_expiry
    ON public.active_leases(lease_expires_at);
