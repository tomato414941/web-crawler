"""Core crawler components."""

from .fetcher import HttpFetcher
from .protocols import Fetcher, Response

__all__ = ["Fetcher", "Response", "HttpFetcher"]

# Lazy imports for optional browser support
def get_browser_fetcher():
    from .browser_fetcher import BrowserFetcher
    return BrowserFetcher

def get_adaptive_fetcher():
    from .adaptive_fetcher import AdaptiveFetcher
    return AdaptiveFetcher
