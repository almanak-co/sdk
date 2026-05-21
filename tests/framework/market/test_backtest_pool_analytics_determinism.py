"""Backtest factories inject NullPoolAnalyticsReader (VIB-4727 D2.M5).

Live gateway HTTP at backtest time = nondeterministic results across runs.
The agreed contract is that both backtest factory functions
(``for_pnl_backtest_state`` / ``for_paper_fork``) inject a
``NullPoolAnalyticsReader`` whose every call raises
``DataSourceUnavailable("backtest")`` — strategies that depend on pool
analytics must take a deterministic code path inside backtests.

These tests also re-assert the source-inspection D1 spec-drift check
(``analytics.py`` has no ``import aiohttp``) and verify via a
socket-connect monkeypatch that the NullPoolAnalyticsReader path makes
no network calls — even latent ones from other imports.
"""

from __future__ import annotations

import socket
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.market_snapshot import PoolAnalyticsUnavailableError
from almanak.framework.data.pools.analytics import NullPoolAnalyticsReader
from almanak.framework.market.builders import MarketSnapshotBuilder

_ANTONIS_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"


# ============================================================================
# for_pnl_backtest_state injects NullPoolAnalyticsReader
# ============================================================================


def test_for_pnl_backtest_state_uses_null_reader():
    """D2.M5: ``MarketSnapshotBuilder.for_pnl_backtest_state(...).pool_analytics(...)``
    raises PoolAnalyticsUnavailableError with __cause__ == DataSourceUnavailable("backtest")."""
    state = MagicMock(price_oracle=None, balance_provider=None)
    snap = MarketSnapshotBuilder.for_pnl_backtest_state(
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000001",
        state=state,
    )

    with pytest.raises(PoolAnalyticsUnavailableError) as wrapped:
        snap.pool_analytics(pool_address=_ANTONIS_POOL, protocol="uniswap_v3")
    assert isinstance(wrapped.value.__cause__, DataSourceUnavailable)
    assert wrapped.value.__cause__.reason == "backtest"


# ============================================================================
# for_paper_fork injects NullPoolAnalyticsReader
# ============================================================================


def test_for_paper_fork_uses_null_reader():
    """D2.M5: ``MarketSnapshotBuilder.for_paper_fork(...).pool_analytics(...)``
    raises the same wrapped DataSourceUnavailable("backtest")."""
    fork_manager = MagicMock(spec=["get_rpc_url", "current_block"])
    fork_manager.get_rpc_url.return_value = "http://127.0.0.1:8545"
    fork_manager.current_block = 100

    snap = MarketSnapshotBuilder.for_paper_fork(
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000001",
        fork_manager=fork_manager,
    )

    with pytest.raises(PoolAnalyticsUnavailableError) as wrapped:
        snap.pool_analytics(pool_address=_ANTONIS_POOL, protocol="uniswap_v3")
    assert isinstance(wrapped.value.__cause__, DataSourceUnavailable)
    assert wrapped.value.__cause__.reason == "backtest"


# ============================================================================
# Re-assert: analytics.py source has no aiohttp import (backslide guard)
# ============================================================================


def test_analytics_module_no_aiohttp_re_check():
    """D2.M5 (source-inspection guard, re-asserted): a regression that
    re-introduced aiohttp into analytics.py would re-open the gateway
    boundary violation PR #2379 was closed for."""
    import almanak.framework.data.pools.analytics as analytics_mod

    src = Path(analytics_mod.__file__).read_text()
    assert "import aiohttp" not in src
    assert "from aiohttp" not in src


# ============================================================================
# NullPoolAnalyticsReader makes no network calls (socket-connect guard)
# ============================================================================


def test_null_reader_does_not_touch_network(monkeypatch: pytest.MonkeyPatch):
    """D2.M5: NullPoolAnalyticsReader must not open any socket — proven
    by a monkeypatched ``socket.socket.connect`` that raises on any call."""

    def _no_connect(self, *_args, **_kwargs):  # noqa: ANN001
        raise RuntimeError("network access during NullPoolAnalyticsReader call")

    monkeypatch.setattr(socket.socket, "connect", _no_connect)

    reader = NullPoolAnalyticsReader()
    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_analytics(
            pool_address=_ANTONIS_POOL,
            chain="arbitrum",
            protocol="uniswap_v3",
        )
    assert excinfo.value.reason == "backtest"
    # If the implementation tried to touch a socket the monkeypatch would
    # have surfaced ``RuntimeError`` instead of the typed DataSourceUnavailable.
