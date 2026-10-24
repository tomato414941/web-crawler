"""Cloudflare R2 storage for fetched page bodies."""

from __future__ import annotations

import gzip
import lzma
import secrets
import time
from typing import Any

from .config import settings

_GZIP_MAGIC = b"\x1f\x8b"
# Bodies are capped at 2 MB, so a larger LZMA window only costs encoder memory.
_XZ_FILTERS = [{"id": lzma.FILTER_LZMA2, "preset": 6, "dict_size": 2 * 1024 * 1024}]


def compress_page_body(body: bytes) -> bytes:
    """Compress one page body as an independent xz record."""
    return lzma.compress(body, format=lzma.FORMAT_XZ, filters=_XZ_FILTERS)


def new_bundle_key() -> str:
    """Return a unique, date-prefixed key for one bundle of page records."""
    return f"bundles/{time.strftime('%Y/%m/%d', time.gmtime())}/{secrets.token_hex(16)}"


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

    def put_bundle(self, key: str, data: bytes) -> None:
        """Write one bundle of concatenated compressed page records."""
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType="application/octet-stream",
        )

    def get_record(self, key: str, offset: int, length: int) -> bytes:
        """Read one page body from its record inside a bundle."""
        response = self._client.get_object(
            Bucket=self._bucket,
            Key=key,
            Range=f"bytes={offset}-{offset + length - 1}",
        )
        return lzma.decompress(response["Body"].read(), format=lzma.FORMAT_XZ)

    def get(self, key: str) -> bytes:
        """Read one page body stored as its own object before bundling."""
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body = response["Body"].read()
        if response.get("ContentEncoding") == "gzip" and body.startswith(_GZIP_MAGIC):
            return gzip.decompress(body)
        return body

    def delete(self, key: str) -> None:
        """Delete one object, such as a released bundle or a replaced single-page body."""
        self._client.delete_object(Bucket=self._bucket, Key=key)
