CREATE INDEX IF NOT EXISTS idx_scheduler_queue_runnable_host_head
    ON public.scheduler_queue_runnable(host, next_fetch_at ASC, priority DESC, added_at ASC, url ASC);

CREATE INDEX IF NOT EXISTS idx_scheduler_queue_scheduled_host_head
    ON public.scheduler_queue_scheduled(host, next_fetch_at ASC, priority DESC, added_at ASC, url ASC);

CREATE INDEX IF NOT EXISTS idx_scheduler_queue_refresh_host_head
    ON public.scheduler_queue_refresh(host, next_fetch_at ASC, priority DESC, added_at ASC, url ASC);

CREATE INDEX IF NOT EXISTS idx_host_runnable_heads_head_url
    ON public.host_runnable_heads(head_url);

WITH selected AS (
    SELECT DISTINCT ON (candidate.host)
        candidate.host,
        candidate.url,
        candidate.next_fetch_at,
        candidate.added_at,
        candidate.priority,
        CASE
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 1000.0 THEN 3
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 400.0 THEN 2
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 150.0 THEN 1
            ELSE 0
        END AS latency_penalty,
        GREATEST(
            candidate.next_fetch_at,
            COALESCE(candidate_host_state.next_request_at, 0),
            COALESCE(candidate_host_state.backoff_until, 0)
        ) AS runnable_at
    FROM public.scheduler_queue_runnable AS candidate
    LEFT JOIN public.host_state AS candidate_host_state
        ON candidate_host_state.host_key = candidate.host
    ORDER BY candidate.host, runnable_at ASC, latency_penalty ASC, candidate.added_at ASC,
        candidate.priority DESC, candidate.url ASC
)
INSERT INTO public.host_runnable_heads (
    physical_queue,
    host,
    head_url,
    head_next_fetch_at,
    head_added_at,
    head_priority,
    runnable_url_count,
    latency_penalty,
    runnable_at,
    refreshed_at
)
SELECT
    'runnable',
    host,
    url,
    next_fetch_at,
    added_at,
    priority,
    1,
    latency_penalty,
    runnable_at,
    EXTRACT(epoch FROM now())
FROM selected
ON CONFLICT (physical_queue, host) DO UPDATE SET
    head_url = EXCLUDED.head_url,
    head_next_fetch_at = EXCLUDED.head_next_fetch_at,
    head_added_at = EXCLUDED.head_added_at,
    head_priority = EXCLUDED.head_priority,
    runnable_url_count = EXCLUDED.runnable_url_count,
    latency_penalty = EXCLUDED.latency_penalty,
    runnable_at = EXCLUDED.runnable_at,
    refreshed_at = EXCLUDED.refreshed_at;

WITH selected AS (
    SELECT DISTINCT ON (candidate.host)
        candidate.host,
        candidate.url,
        candidate.next_fetch_at,
        candidate.added_at,
        candidate.priority,
        CASE
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 1000.0 THEN 3
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 400.0 THEN 2
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 150.0 THEN 1
            ELSE 0
        END AS latency_penalty,
        GREATEST(
            candidate.next_fetch_at,
            COALESCE(candidate_host_state.next_request_at, 0),
            COALESCE(candidate_host_state.backoff_until, 0)
        ) AS runnable_at
    FROM public.scheduler_queue_scheduled AS candidate
    LEFT JOIN public.host_state AS candidate_host_state
        ON candidate_host_state.host_key = candidate.host
    ORDER BY candidate.host, runnable_at ASC, latency_penalty ASC, candidate.added_at ASC,
        candidate.priority DESC, candidate.url ASC
)
INSERT INTO public.host_runnable_heads (
    physical_queue,
    host,
    head_url,
    head_next_fetch_at,
    head_added_at,
    head_priority,
    runnable_url_count,
    latency_penalty,
    runnable_at,
    refreshed_at
)
SELECT
    'scheduled',
    host,
    url,
    next_fetch_at,
    added_at,
    priority,
    1,
    latency_penalty,
    runnable_at,
    EXTRACT(epoch FROM now())
FROM selected
ON CONFLICT (physical_queue, host) DO UPDATE SET
    head_url = EXCLUDED.head_url,
    head_next_fetch_at = EXCLUDED.head_next_fetch_at,
    head_added_at = EXCLUDED.head_added_at,
    head_priority = EXCLUDED.head_priority,
    runnable_url_count = EXCLUDED.runnable_url_count,
    latency_penalty = EXCLUDED.latency_penalty,
    runnable_at = EXCLUDED.runnable_at,
    refreshed_at = EXCLUDED.refreshed_at;

WITH selected AS (
    SELECT DISTINCT ON (candidate.host)
        candidate.host,
        candidate.url,
        candidate.next_fetch_at,
        candidate.added_at,
        candidate.priority,
        CASE
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 1000.0 THEN 3
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 400.0 THEN 2
            WHEN COALESCE(candidate_host_state.latency_ewma_ms, 0) >= 150.0 THEN 1
            ELSE 0
        END AS latency_penalty,
        GREATEST(
            candidate.next_fetch_at,
            COALESCE(candidate_host_state.next_request_at, 0),
            COALESCE(candidate_host_state.backoff_until, 0)
        ) AS runnable_at
    FROM public.scheduler_queue_refresh AS candidate
    LEFT JOIN public.host_state AS candidate_host_state
        ON candidate_host_state.host_key = candidate.host
    ORDER BY candidate.host, runnable_at ASC, latency_penalty ASC, candidate.added_at ASC,
        candidate.priority DESC, candidate.url ASC
)
INSERT INTO public.host_runnable_heads (
    physical_queue,
    host,
    head_url,
    head_next_fetch_at,
    head_added_at,
    head_priority,
    runnable_url_count,
    latency_penalty,
    runnable_at,
    refreshed_at
)
SELECT
    'recrawl',
    host,
    url,
    next_fetch_at,
    added_at,
    priority,
    1,
    latency_penalty,
    runnable_at,
    EXTRACT(epoch FROM now())
FROM selected
ON CONFLICT (physical_queue, host) DO UPDATE SET
    head_url = EXCLUDED.head_url,
    head_next_fetch_at = EXCLUDED.head_next_fetch_at,
    head_added_at = EXCLUDED.head_added_at,
    head_priority = EXCLUDED.head_priority,
    runnable_url_count = EXCLUDED.runnable_url_count,
    latency_penalty = EXCLUDED.latency_penalty,
    runnable_at = EXCLUDED.runnable_at,
    refreshed_at = EXCLUDED.refreshed_at;
