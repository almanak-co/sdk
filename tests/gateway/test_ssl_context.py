"""Tests for the shared SSL context helper."""

import ssl
from unittest.mock import patch

import almanak.gateway.utils.ssl_context as ssl_context_module
from almanak.gateway.utils.ssl_context import build_ssl_context


def setup_function():
    """Reset the module-level singleton before each test."""
    ssl_context_module._ssl_context = None


def test_build_ssl_context_returns_ssl_context():
    """build_ssl_context returns a valid ssl.SSLContext."""
    ctx = build_ssl_context()
    assert isinstance(ctx, ssl.SSLContext)


def test_build_ssl_context_singleton():
    """Repeated calls return the exact same object (singleton)."""
    ctx1 = build_ssl_context()
    ctx2 = build_ssl_context()
    assert ctx1 is ctx2


def test_build_ssl_context_loads_certifi_bundle():
    """certifi.where() is called and load_verify_locations is invoked with it."""
    with patch("certifi.where", return_value="/fake/ca-bundle.crt") as mock_where:
        with patch.object(ssl.SSLContext, "load_verify_locations") as mock_load:
            ctx = build_ssl_context()
            mock_where.assert_called_once()
            mock_load.assert_called_once_with(cafile="/fake/ca-bundle.crt")
            assert isinstance(ctx, ssl.SSLContext)


def test_build_ssl_context_uses_create_default_context():
    """ssl.create_default_context() is called without cafile so system CAs are preserved."""
    original_create = ssl.create_default_context
    calls = []

    def recording_create(*args, **kwargs):
        calls.append(kwargs)
        return original_create(*args, **kwargs)

    with patch("ssl.create_default_context", side_effect=recording_create):
        build_ssl_context()

    assert calls, "ssl.create_default_context was never called"
    assert "cafile" not in calls[0], (
        "cafile must NOT be passed to create_default_context — "
        "it would drop system CA roots"
    )
