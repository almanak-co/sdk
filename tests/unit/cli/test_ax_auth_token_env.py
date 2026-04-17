"""Tests for env-var forwarded auth token in ``almanak ax``.

Bug 5 of the 0G DogFooding report (2026-04-16): when a strategy has started
an auth-enabled managed gateway, operators expect
``ALMANAK_GATEWAY_AUTH_TOKEN`` in ``.env`` to let ``almanak ax`` commands
reach it. Previously the initial probe in ``_get_executor`` passed no
``auth_token`` so every call failed with ``StatusCode.UNAUTHENTICATED``.

These tests verify the token is now read from the environment and
forwarded to ``create_cli_executor``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def base_ctx():
    """Return a click-style ctx.obj dict with the minimal fields ax expects."""
    return {
        "gateway_host": "127.0.0.1",
        "gateway_port": 50055,
        "chain": "zerog",
        "wallet": "0xWALLET",
        "max_trade_usd": 1000.0,
        "network": None,
    }


class _FakeCtx:
    """Minimal stand-in for click.Context — exposes only ``obj``."""

    def __init__(self, data: dict) -> None:
        self.obj = data


class TestAxForwardsAuthTokenFromEnv:
    def test_almanak_gateway_auth_token_is_passed_to_probe(self, base_ctx, monkeypatch):
        """ALMANAK_GATEWAY_AUTH_TOKEN flows into the initial probe call."""
        monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "super-secret-123")
        monkeypatch.delenv("GATEWAY_AUTH_TOKEN", raising=False)

        fake_executor = MagicMock()
        fake_client = MagicMock()

        with patch(
            "almanak.framework.agent_tools.cli_executor.create_cli_executor",
            return_value=(fake_executor, fake_client),
        ) as factory:
            from almanak.framework.cli.ax import _get_executor

            _get_executor(_FakeCtx(base_ctx))

        factory.assert_called_once()
        call = factory.call_args
        assert call.kwargs["auth_token"] == "super-secret-123"
        # Cached for downstream commands in the same CLI session
        assert base_ctx["gateway_auth_token"] == "super-secret-123"

    def test_bare_gateway_auth_token_is_accepted_as_fallback(self, base_ctx, monkeypatch):
        """GATEWAY_AUTH_TOKEN (bare) is used when the prefixed var is absent."""
        monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "fallback-456")

        fake_executor = MagicMock()
        fake_client = MagicMock()

        with patch(
            "almanak.framework.agent_tools.cli_executor.create_cli_executor",
            return_value=(fake_executor, fake_client),
        ) as factory:
            from almanak.framework.cli.ax import _get_executor

            _get_executor(_FakeCtx(base_ctx))

        assert factory.call_args.kwargs["auth_token"] == "fallback-456"

    def test_prefixed_token_wins_over_bare(self, base_ctx, monkeypatch):
        """ALMANAK_GATEWAY_AUTH_TOKEN takes precedence over GATEWAY_AUTH_TOKEN."""
        monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "prefixed")
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "bare")

        fake_executor = MagicMock()
        fake_client = MagicMock()

        with patch(
            "almanak.framework.agent_tools.cli_executor.create_cli_executor",
            return_value=(fake_executor, fake_client),
        ) as factory:
            from almanak.framework.cli.ax import _get_executor

            _get_executor(_FakeCtx(base_ctx))

        assert factory.call_args.kwargs["auth_token"] == "prefixed"

    def test_missing_env_passes_none(self, base_ctx, monkeypatch):
        """No env vars → auth_token=None so insecure gateways keep working."""
        monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
        monkeypatch.delenv("GATEWAY_AUTH_TOKEN", raising=False)

        fake_executor = MagicMock()
        fake_client = MagicMock()

        with patch(
            "almanak.framework.agent_tools.cli_executor.create_cli_executor",
            return_value=(fake_executor, fake_client),
        ) as factory:
            from almanak.framework.cli.ax import _get_executor

            _get_executor(_FakeCtx(base_ctx))

        assert factory.call_args.kwargs["auth_token"] is None
        assert "gateway_auth_token" not in base_ctx
