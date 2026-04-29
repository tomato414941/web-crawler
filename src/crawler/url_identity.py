"""Stable URL identity helpers."""

from __future__ import annotations

URL_IDENTITY_VERSION = 1
MAX_URL_IDENTITY_BYTES = 8192


def url_identity_hash(url: str) -> str:
    """Return the stable hash used by the URL ledger identity checks."""
    # PostgreSQL has a built-in md5(text) function, so invariant checks can
    # compare stored and expected identity hashes without optional extensions.
    import hashlib

    return hashlib.md5(url.encode("utf-8"), usedforsecurity=False).hexdigest()


def url_identity_length(url: str) -> int:
    """Return the UTF-8 byte length used for URL identity bounds."""
    return len(url.encode("utf-8"))
