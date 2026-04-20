"""Shared SSL context for outbound HTTPS connections in the gateway.

Uses certifi's certificate bundle to ensure consistent certificate verification
across environments, particularly on macOS where system certs may differ.
"""

import ssl

_ssl_context: ssl.SSLContext | None = None


def build_ssl_context() -> ssl.SSLContext:
    """Build (or return cached) SSL context using certifi's certificate bundle.

    The context is created once and reused on subsequent calls to avoid
    repeated CA-file loading in hot paths (e.g. backtesting loops).

    Returns:
        ssl.SSLContext configured with certifi certificates
    """
    global _ssl_context
    if _ssl_context is not None:
        return _ssl_context

    import certifi

    context = ssl.create_default_context()
    context.load_verify_locations(cafile=certifi.where())
    _ssl_context = context
    return context
