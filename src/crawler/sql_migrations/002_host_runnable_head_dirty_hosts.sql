CREATE TABLE IF NOT EXISTS public.host_runnable_head_dirty_hosts (
    physical_queue text NOT NULL,
    host text NOT NULL,
    marked_at double precision NOT NULL,
    CONSTRAINT host_runnable_head_dirty_hosts_pkey PRIMARY KEY (physical_queue, host)
);

CREATE INDEX IF NOT EXISTS idx_host_runnable_head_dirty_hosts_marked_at
    ON public.host_runnable_head_dirty_hosts(marked_at);
