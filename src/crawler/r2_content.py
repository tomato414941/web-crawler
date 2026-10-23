"""Cloudflare R2 storage for fetched page bodies."""

from __future__ import annotations

import gzip
from typing import Any

from .config import settings

_GZIP_MAGIC = b"\x1f\x8b"


class R2ContentStore:
    """Store page bodies in one private R2 bucket."""

    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    @classmethod
    def from_settings(cls) -> "R2ContentStore":
        """Build the R2 client from crawler settings."""
        import boto3
        from botocore.config import Config

        endpoint_url, bucket, access_key_id, secret_access_key = settings.r2_config()
        proxy = settings.httpx_proxy()
        config = Config(proxies={"http": proxy, "https": proxy}) if proxy else None
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
            config=config,
        )
        return cls(client, bucket)

    def put(self, key: str, body: bytes, content_type: str) -> None:
        """Write one gzip-compressed page body, replacing any previous body for the URL."""
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=gzip.compress(body, compresslevel=6),
            ContentType=content_type or "application/octet-stream",
            ContentEncoding="gzip",
        )

    def get(self, key: str) -> bytes:
        """Read one page body, including bodies stored before compression."""
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body = response["Body"].read()
        if response.get("ContentEncoding") == "gzip" and body.startswith(_GZIP_MAGIC):
            return gzip.decompress(body)
        return body

    def delete(self, key: str) -> None:
        """Delete a page body when the latest crawl is metadata-only."""
        self._client.delete_object(Bucket=self._bucket, Key=key)
