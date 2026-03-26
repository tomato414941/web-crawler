"""TLS helpers for stable outbound HTTPS verification."""

import ssl

import certifi


def build_ssl_context() -> ssl.SSLContext:
    """Build an SSL context using system trust first, then certifi fallback."""
    paths = ssl.get_default_verify_paths()
    if paths.cafile or paths.capath:
        return ssl.create_default_context(cafile=paths.cafile, capath=paths.capath)
    return ssl.create_default_context(cafile=certifi.where())
