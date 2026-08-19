"""Configuration using pydantic-settings."""

from typing import Annotated, Any

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode


class CrawlerSettings(BaseSettings):
    """Crawler configuration."""

    timeout: float = 10.0
    fetch_body_timeout: float = 10.0
    fetch_total_timeout: float = 35.0
    max_response_body_bytes: int = 2_000_000
    user_agent: str = "WebCrawler/0.1 (+https://github.com/tomato414941/web-crawler)"
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
    admission_target_pending: int = 500_000
    allow_private_network_egress: bool = False
    allowed_egress_ports: Annotated[tuple[int, ...], NoDecode] = (80, 443)
    egress_proxy: str | None = None
    require_egress_proxy: bool = False
    direct_egress_allowed: bool = True
    finalizer_batch_size: int = 16
    finalizer_batch_wait_ms: float = 25.0
    publisher_batch_size: int = 16
    publisher_batch_wait_ms: float = 25.0
    r2_endpoint_url: str | None = None
    r2_bucket: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None

    model_config = {"env_prefix": "CRAWLER_"}

    @field_validator("admission_target_pending")
    @classmethod
    def _validate_admission_target_pending(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("admission_target_pending must be positive")
        return value

    @field_validator("allowed_egress_ports", mode="before")
    @classmethod
    def _validate_allowed_egress_ports(cls, value: Any) -> tuple[int, ...]:
        if isinstance(value, str):
            raw_ports = [part.strip() for part in value.split(",") if part.strip()]
        else:
            raw_ports = list(value)
        ports = tuple(int(port) for port in raw_ports)
        if not ports:
            raise ValueError("allowed_egress_ports must not be empty")
        for port in ports:
            if port < 1 or port > 65535:
                raise ValueError("allowed_egress_ports must contain valid TCP ports")
        return tuple(sorted(set(ports)))

    @model_validator(mode="after")
    def _validate_egress_proxy_policy(self) -> "CrawlerSettings":
        self.validate_egress_transport()
        return self

    def httpx_proxy(self) -> str | None:
        """Return the configured outbound proxy URL, if any."""
        if self.egress_proxy is None:
            return None
        proxy = self.egress_proxy.strip()
        return proxy or None

    def r2_config(self) -> tuple[str, str, str, str]:
        """Return required Cloudflare R2 connection settings."""
        endpoint_url = self.r2_endpoint_url
        bucket = self.r2_bucket
        access_key_id = self.r2_access_key_id
        secret_access_key = self.r2_secret_access_key
        values = (endpoint_url, bucket, access_key_id, secret_access_key)
        if any(not value or not value.strip() for value in values):
            raise ValueError(
                "CRAWLER_R2_ENDPOINT_URL, CRAWLER_R2_BUCKET, "
                "CRAWLER_R2_ACCESS_KEY_ID, and CRAWLER_R2_SECRET_ACCESS_KEY are required"
            )
        assert endpoint_url and bucket and access_key_id and secret_access_key
        return (
            endpoint_url.strip(),
            bucket.strip(),
            access_key_id.strip(),
            secret_access_key.strip(),
        )

    def validate_egress_transport(self) -> None:
        """Fail fast when runtime egress settings cannot be enforced."""
        proxy = self.httpx_proxy()
        if self.require_egress_proxy and proxy is None:
            raise ValueError("CRAWLER_REQUIRE_EGRESS_PROXY=true requires CRAWLER_EGRESS_PROXY")
        if not self.direct_egress_allowed and proxy is None:
            raise ValueError("CRAWLER_DIRECT_EGRESS_ALLOWED=false requires CRAWLER_EGRESS_PROXY")

    def httpx_proxy_kwargs(self) -> dict[str, str | bool]:
        """Return the shared httpx proxy policy for crawler outbound HTTP."""
        self.validate_egress_transport()
        kwargs: dict[str, str | bool] = {"trust_env": False}
        proxy = self.httpx_proxy()
        if proxy is not None:
            kwargs["proxy"] = proxy
        return kwargs


settings = CrawlerSettings()
