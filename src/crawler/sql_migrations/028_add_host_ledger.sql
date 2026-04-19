CREATE TABLE IF NOT EXISTS public.host_ledger (
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

CREATE INDEX IF NOT EXISTS idx_host_ledger_registrable_domain
    ON public.host_ledger(registrable_domain);

CREATE INDEX IF NOT EXISTS idx_host_ledger_last_seen_at
    ON public.host_ledger(last_seen_at);

CREATE INDEX IF NOT EXISTS idx_host_ledger_last_success_at
    ON public.host_ledger(last_success_at);

CREATE INDEX IF NOT EXISTS idx_host_ledger_last_failure_at
    ON public.host_ledger(last_failure_at);

INSERT INTO public.host_ledger (
    host,
    registrable_domain,
    first_seen_at,
    last_seen_at,
    last_success_at,
    last_failure_at,
    known_url_count,
    success_count,
    failure_count,
    created_at,
    updated_at
)
SELECT
    host,
    CASE
        WHEN host LIKE '%.%' THEN regexp_replace(
            split_part(host, ':', 1),
            '^.*[.]([^.]+[.][^.]+)$',
            E'\\1'
        )
        ELSE split_part(host, ':', 1)
    END AS registrable_domain,
    MIN(added_at) AS first_seen_at,
    MAX(GREATEST(
        added_at,
        COALESCE(last_success_at, 0),
        COALESCE(terminalized_at, 0)
    )) AS last_seen_at,
    MAX(last_success_at) AS last_success_at,
    MAX(CASE WHEN terminal_reason IS NOT NULL THEN terminalized_at ELSE NULL END) AS last_failure_at,
    COUNT(*) AS known_url_count,
    COUNT(*) FILTER (WHERE last_success_at IS NOT NULL) AS success_count,
    COUNT(*) FILTER (WHERE terminal_reason IS NOT NULL) AS failure_count,
    MIN(added_at) AS created_at,
    EXTRACT(epoch FROM now()) AS updated_at
FROM public.url_ledger
GROUP BY host
ON CONFLICT (host) DO UPDATE SET
    registrable_domain = COALESCE(
        public.host_ledger.registrable_domain,
        EXCLUDED.registrable_domain
    ),
    first_seen_at = LEAST(public.host_ledger.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at = GREATEST(public.host_ledger.last_seen_at, EXCLUDED.last_seen_at),
    last_success_at = NULLIF(GREATEST(
        COALESCE(public.host_ledger.last_success_at, 0),
        COALESCE(EXCLUDED.last_success_at, 0)
    ), 0),
    last_failure_at = NULLIF(GREATEST(
        COALESCE(public.host_ledger.last_failure_at, 0),
        COALESCE(EXCLUDED.last_failure_at, 0)
    ), 0),
    known_url_count = GREATEST(
        public.host_ledger.known_url_count,
        EXCLUDED.known_url_count
    ),
    success_count = GREATEST(
        public.host_ledger.success_count,
        EXCLUDED.success_count
    ),
    failure_count = GREATEST(
        public.host_ledger.failure_count,
        EXCLUDED.failure_count
    ),
    updated_at = EXCLUDED.updated_at;
