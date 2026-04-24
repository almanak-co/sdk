"""Unit tests for archive-chain startup timeout in ManagedGateway._start_anvil_forks.

Covers VIB-2902 fix: ARCHIVE_RPC_REQUIRED_CHAINS (avalanche, ethereum, polygon) must
receive startup_timeout_seconds=90s while other chains keep 30s.  The timeout must also
propagate to the fallback RollingForkManager path when the primary RPC fails.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any
from unittest.mock import patch

import pytest

from almanak.gateway.managed import ManagedGateway
from almanak.gateway.core.settings import GatewaySettings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**kwargs: Any) -> GatewaySettings:
    return GatewaySettings(
        grpc_port=50099,
        network="anvil",
        allow_insecure=True,
        metrics_enabled=False,
        audit_enabled=False,
        **kwargs,
    )


class _ForkManagerFactory:
    """Creates fake RollingForkManager instances and records constructor kwargs."""

    def __init__(self, primary_succeeds: bool = True, fallback_succeeds: bool = True):
        self.instances: list[_FakeForkManager] = []
        self._primary_succeeds = primary_succeeds
        self._fallback_succeeds = fallback_succeeds
        self._call_index = 0

    def __call__(self, **kwargs: Any) -> "_FakeForkManager":
        idx = self._call_index
        self._call_index += 1
        succeeds = self._primary_succeeds if idx == 0 else self._fallback_succeeds
        inst = _FakeForkManager(kwargs, succeeds=succeeds)
        self.instances.append(inst)
        return inst


class _FakeForkManager:
    def __init__(self, kwargs: dict[str, Any], succeeds: bool = True):
        self.kwargs = kwargs
        self._succeeds = succeeds

    async def start(self) -> bool:
        return self._succeeds

    @property
    def is_running(self) -> bool:
        return self._succeeds


async def _run_start_anvil_forks(
    chain: str,
    factory: _ForkManagerFactory,
    public_rpc_fallback: str | None = None,
) -> None:
    """Run _start_anvil_forks with the given chain, using the provided factory.

    Cleans up any ANVIL_<CHAIN>_PORT env vars that _start_anvil_forks sets,
    so subsequent tests in the same process do not pick up the fake port.
    """
    settings = _make_settings()
    gw = ManagedGateway(settings, anvil_chains=[chain])

    public_rpc_map = {chain: public_rpc_fallback} if public_rpc_fallback else {}

    # Patch the lazy imports that live inside _start_anvil_forks.
    # Because the function does `from X import Y` on each call, we patch at the
    # source module so the re-import picks up our fake.
    import almanak.framework.anvil.fork_manager as _fm_mod
    import almanak.gateway.utils.rpc_provider as _rpc_mod

    orig_rfm = _fm_mod.RollingForkManager
    orig_gru = _rpc_mod.get_rpc_url
    orig_pub = _rpc_mod.PUBLIC_RPC_URLS
    try:
        _fm_mod.RollingForkManager = factory  # type: ignore[attr-defined]
        _rpc_mod.get_rpc_url = lambda chain, network: "https://primary.example.com"  # type: ignore[attr-defined]
        _rpc_mod.PUBLIC_RPC_URLS = public_rpc_map  # type: ignore[attr-defined]

        with patch("almanak.gateway.managed.find_free_port", return_value=19999):
            with patch("shutil.which", return_value="/usr/bin/anvil"):
                await gw._start_anvil_forks()
    finally:
        _fm_mod.RollingForkManager = orig_rfm  # type: ignore[attr-defined]
        _rpc_mod.get_rpc_url = orig_gru  # type: ignore[attr-defined]
        _rpc_mod.PUBLIC_RPC_URLS = orig_pub  # type: ignore[attr-defined]
        # _start_anvil_forks sets ANVIL_<CHAIN>_PORT in os.environ so the
        # gateway RPC provider routes through the fork. Restore the env to
        # avoid leaking the fake port into subsequent tests in the same worker.
        for env_var, original in gw._original_env.items():
            if original is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = original


# ---------------------------------------------------------------------------
# Tests: archive-timeout selection per chain
# ---------------------------------------------------------------------------


class TestArchiveTimeoutSelection:
    """_start_anvil_forks must pass the correct startup_timeout_seconds per chain."""

    @pytest.mark.parametrize(
        "chain,expected_timeout",
        [
            ("avalanche", 90.0),
            ("ethereum", 90.0),
            ("polygon", 90.0),
            ("arbitrum", 30.0),
            ("base", 30.0),
            ("optimism", 30.0),
            ("bsc", 30.0),
        ],
    )
    def test_primary_fork_gets_correct_timeout(
        self, chain: str, expected_timeout: float
    ) -> None:
        """Primary RollingForkManager must receive the archive-aware timeout."""
        factory = _ForkManagerFactory(primary_succeeds=True)
        asyncio.run(_run_start_anvil_forks(chain, factory))

        assert len(factory.instances) == 1, "Expected exactly one RollingForkManager"
        actual = factory.instances[0].kwargs.get("startup_timeout_seconds")
        assert actual == expected_timeout, (
            f"Chain '{chain}': expected {expected_timeout}s, got {actual}s"
        )

    def test_fallback_fork_inherits_same_timeout_as_primary(self) -> None:
        """When primary RPC fails, the fallback RollingForkManager gets the same timeout."""
        chain = "avalanche"
        factory = _ForkManagerFactory(primary_succeeds=False, fallback_succeeds=True)

        asyncio.run(
            _run_start_anvil_forks(
                chain,
                factory,
                public_rpc_fallback="https://fallback.example.com",
            )
        )

        assert len(factory.instances) == 2, (
            f"Expected primary + fallback; got {len(factory.instances)} instances"
        )
        primary_timeout = factory.instances[0].kwargs.get("startup_timeout_seconds")
        fallback_timeout = factory.instances[1].kwargs.get("startup_timeout_seconds")
        assert primary_timeout == 90.0, f"Primary timeout: expected 90s, got {primary_timeout}s"
        assert fallback_timeout == 90.0, f"Fallback timeout: expected 90s, got {fallback_timeout}s"

    def test_fallback_warning_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        """A warning naming the public fallback must be logged before the retry."""
        chain = "avalanche"
        factory = _ForkManagerFactory(primary_succeeds=False, fallback_succeeds=True)

        with caplog.at_level(logging.WARNING, logger="almanak.gateway.managed"):
            asyncio.run(
                _run_start_anvil_forks(
                    chain,
                    factory,
                    public_rpc_fallback="https://public.rpc/avax",
                )
            )

        warning_texts = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("public fallback" in m for m in warning_texts), (
            f"Expected a 'public fallback' warning; got: {warning_texts}"
        )
