"""Configuration using pydantic-settings."""

from pydantic_settings import BaseSettings


class CrawlerSettings(BaseSettings):
    """Crawler configuration."""

    timeout: float = 10.0
    user_agent: str = "WebCrawler/0.1 (+https://github.com/web-crawler)"
    max_connections: int = 100
    max_keepalive_connections: int = 20
    frontier_lease_seconds: float = 300.0
    frontier_retry_backoff_seconds: float = 30.0
    frontier_max_retry_backoff_seconds: float = 1800.0
    robots_cache_ttl: float = 3600.0
    host_backoff_seconds: float = 30.0
    max_host_backoff_seconds: float = 600.0
    daemon_keep_ready_per_domain: int = 128
    daemon_backlog_low_priority: float = 0.75
    daemon_backlog_defer_seconds: float = 1800.0
    daemon_min_ready_sleep: float = 0.5

    model_config = {"env_prefix": "CRAWLER_"}


settings = CrawlerSettings()
