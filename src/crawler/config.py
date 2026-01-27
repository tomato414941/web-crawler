"""Configuration using pydantic-settings."""

from pydantic_settings import BaseSettings


class CrawlerSettings(BaseSettings):
    """Crawler configuration."""

    timeout: float = 10.0
    user_agent: str = "WebCrawler/0.1 (+https://github.com/web-crawler)"
    max_connections: int = 100
    max_keepalive_connections: int = 20

    model_config = {"env_prefix": "CRAWLER_"}


settings = CrawlerSettings()
