"""Shared SSL context for outbound HTTPS connections in the gateway.

Uses certifi's certificate bundle to ensure consistent certificate verification
across environments, particularly on macOS where system certs may differ.
"""

import functools
import ssl


@functools.lru_cache(maxsize=1)
def build_ssl_context() -> ssl.SSLContext:
    """Build (or return cached) SSL context using certifi's certificate bundle.

    The context is created once and reused on subsequent calls to avoid
    repeated CA-file loading in hot paths (e.g. backtesting loops).
    lru_cache makes this thread-safe without an explicit lock.

    Returns:
        ssl.SSLContext configured with certifi certificates
    """
    import certifi

    context = ssl.create_default_context()
    context.load_verify_locations(cafile=certifi.where())
    return context
