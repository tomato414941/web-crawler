"""Configuration using pydantic-settings."""

from pydantic_settings import BaseSettings


class CrawlerSettings(BaseSettings):
    """Crawler configuration."""

    timeout: float = 10.0
    fetch_body_timeout: float = 10.0
    fetch_total_timeout: float = 35.0
    max_response_body_bytes: int = 2_000_000
    user_agent: str = "WebCrawler/0.1 (+https://github.com/web-crawler)"
    max_connections: int = 100
    max_keepalive_connections: int = 20
    max_inflight_requests_per_host: int = 1
    fast_host_latency_threshold_ms: float = 150.0
    fast_host_max_inflight_requests_per_host: int = 2
    scheduler_lease_seconds: float = 300.0
    scheduler_retry_backoff_seconds: float = 30.0
    scheduler_max_retry_backoff_seconds: float = 1800.0
    robots_fetch_timeout: float = 3.0
    robots_cache_ttl: float = 3600.0
    host_backoff_seconds: float = 30.0
    max_host_backoff_seconds: float = 600.0
    daemon_keep_runnable_per_host: int = 128
    daemon_keep_runnable_per_branch: int = 16
    daemon_scheduled_surface_delay_limit: int = 0
    daemon_scheduled_surface_delay_seconds: float = 1800.0
    daemon_min_runnable_sleep: float = 0.5
    daemon_min_runnable_supply_pending: int = 3
    daemon_min_runnable_supply_count: int = 20
    daemon_min_runnable_supply_hosts: int = 8
    daemon_blocked_retry_budget: int = 8
    daemon_blocked_retry_per_host: int = 1
    daemon_blocked_retry_max_consecutive_failures: int = 8
    daemon_quarantine_retire_min_consecutive_failures: int = 64
    daemon_quarantine_retire_after_seconds: float = 86400.0
    daemon_host_head_repair_limit: int = 64
    daemon_host_head_dirty_refresh_limit: int = 2048
    execution_probing_worker_ratio: float = 0.2

    model_config = {"env_prefix": "CRAWLER_"}


settings = CrawlerSettings()
