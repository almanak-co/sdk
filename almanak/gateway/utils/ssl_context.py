"""Shared SSL context for outbound HTTPS connections in the gateway.

Uses certifi's certificate bundle to ensure consistent certificate verification
across environments, particularly on macOS where system certs may differ.
"""

import ssl


def build_ssl_context() -> ssl.SSLContext:
    """Build an SSL context using certifi's certificate bundle.

    Falls back to the default SSL context if certifi is unavailable.

    Returns:
        ssl.SSLContext configured with certifi certificates
    """
    context = ssl.create_default_context()
    try:
        import certifi

        context.load_verify_locations(cafile=certifi.where())
    except ImportError:
        pass
    return context
