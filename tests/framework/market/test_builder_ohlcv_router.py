"""VIB-4347: ``MarketSnapshotBuilder.for_strategy_runner`` threads ``ohlcv_router``.

Live runner stamps ``strategy._ohlcv_router`` during indicator wiring; the
builder must lift it onto the snapshot so ``market.ohlcv(...)`` resolves to
the same routed gateway-backed pipes the indicator path uses. Out of this
wiring, ``MarketSnapshot.ohlcv()`` falls back to the legacy ohlcv_module
path and rejects pool-scoped calls — silent drift to the wrong data path.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.market import MarketSnapshotBuilder


def _stub_strategy(ohlcv_router=None) -> SimpleNamespace:
    """Minimal duck-typed strategy with the wired-providers attributes the
    builder reads. ``ohlcv_router`` is the only field varied across tests."""
    strategy = SimpleNamespace()
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x" + "0" * 40
    strategy._ohlcv_router = ohlcv_router
    # The builder calls getattr(strategy, "<provider>", None) for these — they
    # default to None when absent, so we don't have to set them all.
    return strategy


# =============================================================================
# D1.3 — Single-chain build preserves ohlcv_router
# =============================================================================


def test_for_strategy_runner_preserves_ohlcv_router() -> None:
    fake_router = MagicMock(name="OHLCVRouter")
    strategy = _stub_strategy(ohlcv_router=fake_router)

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, chain="arbitrum")
    assert snap._ohlcv_router is fake_router


# =============================================================================
# D1.3 — Multi-chain build preserves ohlcv_router
# =============================================================================


def test_for_strategy_runner_preserves_ohlcv_router_multichain() -> None:
    fake_router = MagicMock(name="OHLCVRouter")
    strategy = _stub_strategy(ohlcv_router=fake_router)
    # Multi-chain providers are pulled off the strategy via getattr — defaulting
    # to None is fine for this test, the builder doesn't dereference them.
    snap = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy,
        chain="arbitrum",
        chains=("arbitrum", "base"),
    )
    assert snap._ohlcv_router is fake_router


# =============================================================================
# F4 — Absent _ohlcv_router doesn't crash the builder
# =============================================================================


def test_for_strategy_runner_ohlcv_router_absent() -> None:
    """Strategies that opted out of indicators (or didn't have the runner wire
    them) must build a snapshot with ohlcv_router=None — NOT crash."""
    strategy = SimpleNamespace()
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x" + "0" * 40
    # Note: no ``_ohlcv_router`` attribute at all — getattr's default kicks in.

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, chain="arbitrum")
    assert snap._ohlcv_router is None


def test_for_strategy_runner_ohlcv_router_explicit_none() -> None:
    """Same shape, but the attribute exists and is explicitly None."""
    strategy = _stub_strategy(ohlcv_router=None)

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, chain="arbitrum")
    assert snap._ohlcv_router is None
