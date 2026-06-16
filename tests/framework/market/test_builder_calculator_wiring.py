"""VIB-4844 (Epic B, T3-B): MarketSnapshotBuilder stateless-calculator wiring.

Before this ticket, ``il_calculator`` / ``volatility_calculator`` /
``risk_calculator`` were accepted by ``MarketSnapshot.__init__`` but never
constructed by any builder factory, so the documented surface
(``il_exposure`` / ``projected_il`` / ``realized_vol`` / ``vol_cone`` /
``portfolio_risk`` / ``rolling_sharpe``) raised
``ValueError("No X calculator configured for MarketSnapshot")`` at runtime —
dead API.

These calculators are pure Python (no gateway, no egress, no secrets) so the
PRD §Epic B T3-B contract wires the *real* instances on **every** surface,
including the backtest factories — they are deterministic math over series the
snapshot already holds, so they do not break replay reproducibility (unlike a
live gateway/HTTP-backed provider, which is why pool analytics / pool history
get ``Null*`` stubs instead).

This module sweeps each builder factory × each newly-wired calculator and
asserts the calculators are present and produce real data (not the
not-configured ``ValueError``).

T3-E (gas oracle + Solana LST single-call providers) is NOT wired by this
ticket — there is no gateway gas-oracle / LST service to construct from
``gateway_client`` and the existing implementations do raw HTTP/RPC egress
(gateway-boundary forbidden in the strategy container). The gas-oracle
methods therefore remain unconfigured; that is asserted here so the blocked
state is explicit and any future wiring trips this test.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.market.builders import MarketSnapshotBuilder

# --- factory fixtures --------------------------------------------------------


class _FakeStrategy:
    """Minimal strategy object — no wired providers, no calculators.

    The builder must construct the stateless calculators itself.
    """

    chain = "base"
    wallet_address = "0x" + "1" * 40


class _FakeBacktestState:
    """Minimal pnl-backtest state object."""

    timestamp = datetime(2026, 5, 1, tzinfo=UTC)


class _FakeForkManager:
    def get_rpc_url(self) -> str:
        return "http://127.0.0.1:8545"

    current_block = 1


class _FakeHttpSpec:
    chain = "base"
    wallet_address = "0x" + "2" * 40
    timestamp = datetime(2026, 5, 1, tzinfo=UTC)


def _snapshot_strategy_runner():
    return MarketSnapshotBuilder.for_strategy_runner(strategy=_FakeStrategy(), chain="base")


def _snapshot_pnl_backtest():
    return MarketSnapshotBuilder.for_pnl_backtest_state(
        chain="base",
        wallet_address="0x" + "3" * 40,
        state=_FakeBacktestState(),
    )


def _snapshot_paper_fork():
    return MarketSnapshotBuilder.for_paper_fork(
        chain="base",
        wallet_address="0x" + "4" * 40,
        fork_manager=_FakeForkManager(),
    )


def _snapshot_http_backtest():
    return MarketSnapshotBuilder.for_http_backtest_spec(spec=_FakeHttpSpec())


_FACTORIES = [
    pytest.param(_snapshot_strategy_runner, id="for_strategy_runner"),
    pytest.param(_snapshot_pnl_backtest, id="for_pnl_backtest_state"),
    pytest.param(_snapshot_paper_fork, id="for_paper_fork"),
    pytest.param(_snapshot_http_backtest, id="for_http_backtest_spec"),
]


# Inputs for the pure-math methods that need a series long enough to clear the
# 30-observation floor in the risk calculator.
_PNL_SERIES = [0.01, -0.005, 0.012, -0.008, 0.003] * 8  # 40 observations


# --- calculator presence -----------------------------------------------------


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_calculators_constructed_on_every_factory(make_snapshot) -> None:
    """Every builder factory wires all three stateless calculators."""
    snap = make_snapshot()
    assert snap._il_calculator is not None
    assert snap._volatility_calculator is not None
    assert snap._risk_calculator is not None


# --- newly-unblocked methods return real data, not the not-configured error --


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_projected_il_returns_real_data(make_snapshot) -> None:
    """``projected_il`` (pure math, no position/infra needed) returns a real
    result on every surface instead of raising the not-configured ValueError.
    """
    snap = make_snapshot()
    result = snap.projected_il(token_a="WETH", token_b="USDC", price_change_pct=Decimal("50"))
    # +50% price move on a 50/50 constant-product pool yields a non-trivial IL.
    assert result.il_percent < 0  # IL is a loss
    assert result.il_ratio != Decimal("0")


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_portfolio_risk_returns_real_data(make_snapshot) -> None:
    """``portfolio_risk`` computes from a caller-supplied PnL series — needs no
    infra, so it must return a real envelope on every surface.
    """
    snap = make_snapshot()
    env = snap.portfolio_risk(pnl_series=_PNL_SERIES, total_value_usd=Decimal("10000"))
    assert env.value is not None
    # sharpe_ratio is part of PortfolioRisk; a non-degenerate series gives a
    # finite value.
    assert hasattr(env.value, "sharpe_ratio")


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_rolling_sharpe_returns_real_data(make_snapshot) -> None:
    """``rolling_sharpe`` computes from a PnL series — real data on every
    surface."""
    snap = make_snapshot()
    timestamps = [datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=i) for i in range(len(_PNL_SERIES))]
    env = snap.rolling_sharpe(pnl_series=_PNL_SERIES, window_days=10, timestamps=timestamps)
    assert env.value is not None


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_il_exposure_reaches_calculator_not_config_error(make_snapshot) -> None:
    """``il_exposure`` is wired: an unknown position id surfaces a *position*
    error (ILExposureUnavailableError), NOT the "No IL calculator configured"
    ValueError that the unwired surface produced.
    """
    from almanak.framework.data.market_snapshot import ILExposureUnavailableError

    snap = make_snapshot()
    with pytest.raises(ILExposureUnavailableError):
        snap.il_exposure(position_id="does-not-exist")


# --- VIB-5153 / ALM-2814: il_exposure soft-fallback via default= -------------


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_il_exposure_default_none_soft_returns(make_snapshot) -> None:
    """When ``default=None`` is supplied, an unavailable IL exposure returns
    ``None`` instead of raising — so a defensive strategy
    (``il = market.il_exposure(pid, default=None); if il is None: hold``)
    does not let the failure escape into the runner circuit breaker.
    """
    snap = make_snapshot()
    assert snap.il_exposure(position_id="does-not-exist", default=None) is None


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_il_exposure_default_sentinel_value_returned(make_snapshot) -> None:
    """An arbitrary ``default`` object is returned verbatim on failure (None is
    not special — it is just one possible default)."""
    snap = make_snapshot()
    sentinel = object()
    assert snap.il_exposure(position_id="does-not-exist", default=sentinel) is sentinel


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_il_exposure_no_default_still_raises(make_snapshot) -> None:
    """Back-compat: omitting ``default`` preserves the historical raising
    contract exactly (existing callers are unaffected)."""
    from almanak.framework.data.market_snapshot import ILExposureUnavailableError

    snap = make_snapshot()
    with pytest.raises(ILExposureUnavailableError):
        snap.il_exposure(position_id="does-not-exist")


def test_il_exposure_default_does_not_mask_missing_calculator() -> None:
    """A missing IL calculator is a wiring error, not transient data — it must
    raise ``ValueError`` even when ``default`` is supplied, so a broken strategy
    is never silently degraded to ``default`` forever."""
    from almanak.framework.market.snapshot import MarketSnapshot

    snap = MarketSnapshot(chain="base", wallet_address="0x" + "5" * 40)
    assert snap._il_calculator is None
    with pytest.raises(ValueError, match="No IL calculator configured"):
        snap.il_exposure(position_id="anything", default=None)


def test_il_exposure_default_does_not_mask_unexpected_error() -> None:
    """An *unexpected* error inside the calculator path (not typed transient
    IL-unavailability) is a misconfiguration / upstream bug and must stay loud
    even when ``default`` is supplied — it is wrapped as
    ``ILExposureUnavailableError``, NOT silently degraded to ``default``.
    ``default`` is reserved for the typed transient branches only
    (VIB-5153 / ALM-2814; CodeRabbit review of PR #2841)."""
    from almanak.framework.data.market_snapshot import ILExposureUnavailableError
    from almanak.framework.market.snapshot import MarketSnapshot

    class _BoomCalculator:
        def get_position(self, position_id: str):
            raise RuntimeError("upstream boom")

    snap = MarketSnapshot(chain="base", wallet_address="0x" + "5" * 40)
    snap._il_calculator = _BoomCalculator()  # type: ignore[assignment]
    with pytest.raises(ILExposureUnavailableError, match="Unexpected error"):
        snap.il_exposure(position_id="anything", default=None)


# --- T3-E (gas oracle) is explicitly NOT wired by this ticket ----------------


@pytest.mark.parametrize("make_snapshot", _FACTORIES)
def test_gas_oracle_remains_unconfigured_blocked_on_gateway(make_snapshot) -> None:
    """T3-E is blocked (no gateway gas-oracle service). The gas oracle must
    stay unconfigured on every surface; ``gas_price`` raises the
    not-configured ValueError. If a future change wires a gas oracle, this
    test trips so the T3-A stopgap and this blocked-state note get revisited.
    """
    snap = make_snapshot()
    assert snap._gas_oracle is None
    with pytest.raises(ValueError, match="No gas oracle configured"):
        snap.gas_price("base")
