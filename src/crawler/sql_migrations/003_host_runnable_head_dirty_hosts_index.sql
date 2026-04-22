DROP INDEX IF EXISTS public.idx_host_runnable_head_dirty_hosts_marked_at;

CREATE INDEX IF NOT EXISTS idx_host_runnable_head_dirty_hosts_queue_marked_at_host
    ON public.host_runnable_head_dirty_hosts(physical_queue, marked_at, host);
