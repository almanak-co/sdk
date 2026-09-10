"""Declared guard evidence stays honest before historical providers are called."""

import pytest

from almanak.framework.backtesting.pnl.dependencies import check_declared_dependencies
from tests.unit.backtesting.pnl.test_dependency_review_regressions import dependency
from tests.unit.backtesting.pnl.test_readiness import _backtester, _config, _Provider, _Strategy


class Strategy(_Strategy):
    changes = {}

    def backtest_data_dependencies(self, config):
        return (dependency(config, **self.changes), dependency(config, dependency_id="other"))


@pytest.mark.asyncio
async def test_invalid_typed_declaration_retains_unverified_rows():
    strategy = Strategy()
    strategy.changes = {"pool_address": "0xnothex"}
    result = await _backtester(_Provider()).check_readiness(strategy, _config())
    assert not result.ready
    assert len(result.dependency_coverage) == 2
    assert all(row["state"] is None for row in result.dependency_coverage)
    assert all(row["provenance"] == "declared" for row in result.dependency_coverage)
    assert "Invalid declaration" in result.dependency_coverage[0]["detail"]


@pytest.mark.asyncio
async def test_scalar_freshness_refuses_before_fetch(monkeypatch):
    def forbidden():
        pytest.fail("An unsupported observation-age contract must not fetch history")

    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", forbidden)
    strategy = Strategy()
    strategy.changes = {"max_staleness_seconds": 120}
    result = await _backtester(_Provider()).check_readiness(strategy, _config())
    assert not result.ready
    assert result.dependency_coverage[0]["state"] == "unsupported_capability"
    assert "observation age" in result.dependency_coverage[0]["detail"]
    assert result.dependency_coverage[1]["state"] is None


@pytest.mark.asyncio
async def test_altered_unsupported_guard_is_explicitly_unverified():
    config = _config()
    config.altered_backtest_guards = {"volume": "Compare the signal without depth"}
    strategy = Strategy()
    strategy.changes = {"lane": "liquidity_depth", "required_fidelity": "initialized_ticks"}
    coverage = await check_declared_dependencies(strategy, config)
    assert coverage[0].state is None
    assert "Compare the signal without depth" in coverage[0].detail


@pytest.mark.asyncio
async def test_mutated_reason_cannot_bypass_consumption_boundary():
    config = _config()
    config.altered_backtest_guards = {"volume": ""}
    with pytest.raises(ValueError, match="reason"):
        await check_declared_dependencies(Strategy(), config, (dependency(config),))


@pytest.mark.asyncio
async def test_normalized_tvl_freshness_is_not_statically_refused():
    config = _config()
    coverage = await check_declared_dependencies(
        Strategy(), config, (dependency(config, required_fidelity=" tvl_usd ", max_staleness_seconds=120),)
    )
    assert coverage[0].state is None


@pytest.mark.asyncio
async def test_other_guard_failure_does_not_relabel_altered_coverage(monkeypatch):
    class History:
        def daily_history(self, **kwargs):
            return None

    monkeypatch.setattr("almanak.framework.backtesting.pnl.data_broker.pool_history_provider", lambda: History())
    config = _config()
    config.altered_backtest_guards = {"volume": "Compare without this guard"}
    result = await _backtester(_Provider()).check_readiness(Strategy(), config)
    assert not result.ready
    assert result.dependency_coverage[0]["state"] is None
    assert "Compare without this guard" in result.dependency_coverage[0]["detail"]
    assert result.dependency_coverage[1]["state"] == "incomplete_historical_coverage"
