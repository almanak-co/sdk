"""Tests for real risk metric calculations in PolicyEngine and ToolExecutor.

Tests cover:
- PortfolioSnapshot recording and rolling window management
- Max drawdown calculation from known peak-trough patterns
- Rolling volatility with known return series
- Sharpe ratio with known mean/std returns
- Historical VaR (95%) with known distribution
- Edge cases: empty history, single point, zero values
- Integration with _execute_get_risk_metrics (backward compat)
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy, PolicyEngine, PortfolioSnapshot


# =============================================================================
# Helpers
# =============================================================================


def _make_engine(**policy_kwargs) -> PolicyEngine:
    """Create a PolicyEngine with permissive defaults for testing."""
    policy = AgentPolicy(
        allowed_chains={"arbitrum", "base", "ethereum"},
        cooldown_seconds=0,
        max_tool_calls_per_minute=100,
        **policy_kwargs,
    )
    return PolicyEngine(policy)


def _seed_snapshots(engine: PolicyEngine, values: list[Decimal | float | int]) -> None:
    """Inject portfolio value snapshots into the engine.

    Each value becomes a snapshot spaced 1 day apart, simulating daily observations.
    Uses update_portfolio_value() to go through the real code path.
    """
    for val in values:
        engine.update_portfolio_value(Decimal(str(val)))


def _seed_snapshots_raw(engine: PolicyEngine, snapshots: list[PortfolioSnapshot]) -> None:
    """Directly inject PortfolioSnapshot objects for precise timestamp control."""
    engine._portfolio_snapshots = list(snapshots)
    if snapshots:
        engine._current_portfolio_usd = snapshots[-1].value_usd
        engine._peak_portfolio_usd = max(s.value_usd for s in snapshots)


# =============================================================================
# PortfolioSnapshot Dataclass
# =============================================================================


class TestPortfolioSnapshot:
    def test_fields(self):
        ts = datetime.now(UTC)
        snap = PortfolioSnapshot(timestamp=ts, value_usd=Decimal("1000.50"))
        assert snap.timestamp == ts
        assert snap.value_usd == Decimal("1000.50")


# =============================================================================
# Snapshot Recording
# =============================================================================


class TestSnapshotRecording:
    def test_update_portfolio_value_records_snapshot(self):
        engine = _make_engine()
        engine.update_portfolio_value(Decimal("10000"))
        assert len(engine._portfolio_snapshots) == 1
        assert engine._portfolio_snapshots[0].value_usd == Decimal("10000")

    def test_multiple_updates_accumulate(self):
        engine = _make_engine()
        for v in [100, 200, 300]:
            engine.update_portfolio_value(Decimal(str(v)))
        assert len(engine._portfolio_snapshots) == 3

    def test_rolling_window_capped(self):
        engine = _make_engine()
        engine._max_snapshots = 5
        for i in range(10):
            engine.update_portfolio_value(Decimal(str(1000 + i)))
        assert len(engine._portfolio_snapshots) == 5
        # Should keep the last 5
        assert engine._portfolio_snapshots[0].value_usd == Decimal("1005")
        assert engine._portfolio_snapshots[-1].value_usd == Decimal("1009")

    def test_high_water_mark_updates(self):
        engine = _make_engine()
        engine.update_portfolio_value(Decimal("100"))
        engine.update_portfolio_value(Decimal("200"))
        engine.update_portfolio_value(Decimal("150"))
        assert engine._peak_portfolio_usd == Decimal("200")

    def test_portfolio_snapshots_property_returns_copy(self):
        engine = _make_engine()
        engine.update_portfolio_value(Decimal("100"))
        snaps = engine.portfolio_snapshots
        snaps.clear()  # mutating the copy
        assert len(engine._portfolio_snapshots) == 1  # original untouched


# =============================================================================
# Max Drawdown
# =============================================================================


class TestMaxDrawdown:
    def test_no_snapshots_returns_zero(self):
        engine = _make_engine()
        assert engine.calculate_max_drawdown() == Decimal("0")

    def test_single_snapshot_returns_zero(self):
        engine = _make_engine()
        _seed_snapshots(engine, [1000])
        assert engine.calculate_max_drawdown() == Decimal("0")

    def test_monotonic_increase_no_drawdown(self):
        """Strictly increasing portfolio has zero drawdown."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 200, 300, 400, 500])
        assert engine.calculate_max_drawdown() == Decimal("0")

    def test_known_drawdown(self):
        """Peak 200, trough 100 = 50% drawdown."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 200, 100])
        dd = engine.calculate_max_drawdown()
        assert dd == Decimal("0.5")

    def test_drawdown_with_recovery(self):
        """Peak 200, trough 100, recovery to 250. Max DD still 50%."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 200, 100, 250])
        dd = engine.calculate_max_drawdown()
        assert dd == Decimal("0.5")

    def test_multiple_drawdowns_takes_max(self):
        """Two drawdowns: 20% then 50%. Should report 50%."""
        engine = _make_engine()
        # Peak 100 -> 80 (20% DD), recovery to 200 -> 100 (50% DD)
        _seed_snapshots(engine, [100, 80, 200, 100])
        dd = engine.calculate_max_drawdown()
        assert dd == Decimal("0.5")

    def test_small_drawdown_precision(self):
        """1% drawdown should be captured precisely."""
        engine = _make_engine()
        _seed_snapshots(engine, [10000, 9900])
        dd = engine.calculate_max_drawdown()
        assert dd == Decimal("0.01")

    def test_full_loss(self):
        """Portfolio goes to zero = 100% drawdown."""
        engine = _make_engine()
        _seed_snapshots(engine, [1000, 0])
        dd = engine.calculate_max_drawdown()
        assert dd == Decimal("1")


# =============================================================================
# Volatility
# =============================================================================


class TestVolatility:
    def test_insufficient_data_returns_zero(self):
        """Need at least 3 snapshots (2 returns) for volatility."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 200])
        assert engine.calculate_volatility() == Decimal("0")

    def test_no_data_returns_zero(self):
        engine = _make_engine()
        assert engine.calculate_volatility() == Decimal("0")

    def test_constant_portfolio_zero_volatility(self):
        """Constant value = zero returns = zero volatility."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 100, 100, 100, 100])
        assert engine.calculate_volatility() == Decimal("0")

    def test_known_volatility(self):
        """Known return series with calculable volatility.

        Values: 100 -> 110 -> 99 -> 108.9 -> 97.02
        Returns: +10%, -10%, +10%, -10.9%  (approximately alternating)
        """
        engine = _make_engine()
        # Use exact values for reproducible returns
        _seed_snapshots(engine, [100, 110, 99, 108.9])
        vol = engine.calculate_volatility()
        assert vol > Decimal("0")
        # Annualized vol should be reasonable (not astronomical)
        assert vol < Decimal("10")

    def test_positive_for_varying_portfolio(self):
        """Any varying portfolio should have positive volatility."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 105, 95, 110, 90, 100])
        vol = engine.calculate_volatility()
        assert vol > Decimal("0")

    def test_annualization_factor(self):
        """Different annualization factor scales result."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 110, 90, 105, 95, 100])
        vol_daily = engine.calculate_volatility(annualization_factor=252)
        vol_hourly = engine.calculate_volatility(annualization_factor=8760)
        # Hourly annualization should produce higher value
        assert vol_hourly > vol_daily


# =============================================================================
# Sharpe Ratio
# =============================================================================


class TestSharpeRatio:
    def test_insufficient_data_returns_zero(self):
        engine = _make_engine()
        _seed_snapshots(engine, [100, 200])
        assert engine.calculate_sharpe() == Decimal("0")

    def test_no_data_returns_zero(self):
        engine = _make_engine()
        assert engine.calculate_sharpe() == Decimal("0")

    def test_constant_portfolio_zero_sharpe(self):
        """Zero volatility -> zero Sharpe (avoid division by zero)."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 100, 100, 100])
        assert engine.calculate_sharpe() == Decimal("0")

    def test_positive_returns_positive_sharpe(self):
        """Steadily growing portfolio with some noise should have positive Sharpe."""
        engine = _make_engine()
        # Growth with slight variation to avoid zero stdev
        multipliers = [
            "1.012", "1.008", "1.015", "1.009", "1.011",
            "1.013", "1.007", "1.014", "1.010", "1.012",
            "1.008", "1.016", "1.009", "1.011", "1.013",
            "1.007", "1.015", "1.010", "1.012", "1.008",
        ]
        values = [Decimal("100")]
        for m in multipliers:
            values.append(values[-1] * Decimal(m))
        _seed_snapshots(engine, values)
        sharpe = engine.calculate_sharpe()
        assert sharpe > Decimal("0")

    def test_negative_returns_negative_sharpe(self):
        """Steadily declining portfolio with some noise should have negative Sharpe."""
        engine = _make_engine()
        # Decline with slight variation to avoid zero stdev
        multipliers = [
            "0.988", "0.992", "0.985", "0.991", "0.989",
            "0.987", "0.993", "0.986", "0.990", "0.988",
            "0.992", "0.984", "0.991", "0.989", "0.987",
            "0.993", "0.985", "0.990", "0.988", "0.992",
        ]
        values = [Decimal("100")]
        for m in multipliers:
            values.append(values[-1] * Decimal(m))
        _seed_snapshots(engine, values)
        sharpe = engine.calculate_sharpe()
        assert sharpe < Decimal("0")

    def test_high_sharpe_for_low_vol_high_return(self):
        """Very consistent positive returns with tiny noise should give high Sharpe."""
        engine = _make_engine()
        # High mean return with very low variance
        multipliers = [
            "1.0105", "1.0095", "1.0102", "1.0098", "1.0101",
            "1.0099", "1.0103", "1.0097", "1.0104", "1.0096",
            "1.0102", "1.0098", "1.0101", "1.0099", "1.0103",
            "1.0097", "1.0105", "1.0095", "1.0102", "1.0098",
            "1.0101", "1.0099", "1.0103", "1.0097", "1.0104",
            "1.0096", "1.0102", "1.0098", "1.0101", "1.0099",
        ]
        values = [Decimal("10000")]
        for m in multipliers:
            values.append(values[-1] * Decimal(m))
        _seed_snapshots(engine, values)
        sharpe = engine.calculate_sharpe()
        assert sharpe > Decimal("1")

    def test_risk_free_rate_affects_sharpe(self):
        """Higher risk-free rate should lower Sharpe ratio."""
        engine = _make_engine()
        # Growth with noise so stdev != 0
        multipliers = [
            "1.006", "1.004", "1.007", "1.003", "1.005",
            "1.006", "1.004", "1.008", "1.003", "1.005",
            "1.007", "1.004", "1.006", "1.003", "1.005",
            "1.006", "1.004", "1.007", "1.003", "1.005",
        ]
        values = [Decimal("100")]
        for m in multipliers:
            values.append(values[-1] * Decimal(m))
        _seed_snapshots(engine, values)
        sharpe_low_rf = engine.calculate_sharpe(risk_free_rate=0.01)
        sharpe_high_rf = engine.calculate_sharpe(risk_free_rate=0.10)
        assert sharpe_low_rf > sharpe_high_rf


# =============================================================================
# VaR (95%)
# =============================================================================


class TestVaR95:
    def test_insufficient_data_returns_zero(self):
        """Need at least 10 snapshots (9 returns) for VaR."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 200, 300, 400, 500])
        assert engine.calculate_var_95() == Decimal("0")

    def test_no_data_returns_zero(self):
        engine = _make_engine()
        assert engine.calculate_var_95() == Decimal("0")

    def test_constant_portfolio_zero_var(self):
        """No variation = zero VaR."""
        engine = _make_engine()
        _seed_snapshots(engine, [100] * 15)
        assert engine.calculate_var_95() == Decimal("0")

    def test_known_distribution(self):
        """VaR from a known return distribution.

        20 returns of alternating +5% / -3%. Sorted returns would have
        -3% as the worst. 5th percentile of 20 returns = index 1 -> -3%.
        """
        engine = _make_engine()
        # Create 21 snapshots for 20 returns alternating +5% / -3%
        values = [Decimal("1000")]
        for i in range(20):
            if i % 2 == 0:
                values.append(values[-1] * Decimal("1.05"))
            else:
                values.append(values[-1] * Decimal("0.97"))
        _seed_snapshots(engine, values)
        var = engine.calculate_var_95()
        assert var > Decimal("0")
        # VaR should capture the ~3% downside moves
        assert var <= Decimal("0.05")

    def test_all_positive_returns(self):
        """All positive returns: worst return is still small, VaR is small."""
        engine = _make_engine()
        values = [Decimal("1000")]
        for _ in range(15):
            values.append(values[-1] * Decimal("1.02"))  # +2% each period
        _seed_snapshots(engine, values)
        var = engine.calculate_var_95()
        # All returns are positive, so the 5th percentile is a small positive return
        # VaR is abs() of that, so it should be very small (~0.02)
        assert var >= Decimal("0")

    def test_var_increases_with_more_volatile_portfolio(self):
        """Higher volatility portfolio should have higher VaR."""
        # Low vol portfolio: +1% / -1% alternating
        engine_low = _make_engine()
        vals_low = [Decimal("1000")]
        for i in range(20):
            mult = Decimal("1.01") if i % 2 == 0 else Decimal("0.99")
            vals_low.append(vals_low[-1] * mult)
        _seed_snapshots(engine_low, vals_low)

        # High vol portfolio: +10% / -10% alternating
        engine_high = _make_engine()
        vals_high = [Decimal("1000")]
        for i in range(20):
            mult = Decimal("1.10") if i % 2 == 0 else Decimal("0.90")
            vals_high.append(vals_high[-1] * mult)
        _seed_snapshots(engine_high, vals_high)

        var_low = engine_low.calculate_var_95()
        var_high = engine_high.calculate_var_95()
        assert var_high > var_low


# =============================================================================
# get_risk_metrics (aggregated)
# =============================================================================


class TestGetRiskMetrics:
    def test_empty_portfolio(self):
        engine = _make_engine()
        metrics = engine.get_risk_metrics()
        assert metrics["portfolio_value_usd"] == "0"
        assert metrics["max_drawdown_pct"] == "0"
        assert metrics["volatility_annualized"] == "0"
        assert metrics["sharpe_ratio"] == "0"
        assert metrics["var_95_pct"] == "0"
        assert metrics["data_points"] == 0
        assert metrics["data_sufficient"] is False
        assert len(metrics["warnings"]) == 2  # insufficient for vol + VaR

    def test_sufficient_data(self):
        engine = _make_engine()
        # Seed 15 snapshots
        values = [Decimal("1000")]
        for i in range(14):
            mult = Decimal("1.02") if i % 2 == 0 else Decimal("0.98")
            values.append(values[-1] * mult)
        _seed_snapshots(engine, values)
        metrics = engine.get_risk_metrics()
        assert metrics["data_points"] == 15
        assert metrics["data_sufficient"] is True
        assert metrics["warnings"] == []
        # All metrics should have non-zero string values
        assert metrics["max_drawdown_pct"] != "0"
        assert metrics["volatility_annualized"] != "0"
        assert metrics["sharpe_ratio"] != "0"
        assert metrics["var_95_pct"] != "0"

    def test_partial_data_warnings(self):
        """3-9 snapshots: vol/Sharpe work but VaR insufficient."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 110, 90, 105, 95])
        metrics = engine.get_risk_metrics()
        assert metrics["data_points"] == 5
        assert metrics["data_sufficient"] is False
        # Should warn about VaR but not about vol/Sharpe
        assert any("VaR" in w for w in metrics["warnings"])
        assert not any("volatility" in w for w in metrics["warnings"])

    def test_data_points_matches_snapshot_count(self):
        engine = _make_engine()
        _seed_snapshots(engine, [100, 200, 300])
        metrics = engine.get_risk_metrics()
        assert metrics["data_points"] == 3


# =============================================================================
# Integration: _execute_get_risk_metrics (executor)
# =============================================================================


class TestExecuteGetRiskMetrics:
    @pytest.fixture
    def mock_gateway(self):
        client = MagicMock()
        client.is_connected = True
        return client

    @pytest.fixture
    def executor(self, mock_gateway):
        policy = AgentPolicy(
            allowed_chains={"arbitrum", "base", "ethereum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
            require_rebalance_check=False,
        )
        return ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
        )

    def _mock_balances(self, mock_gateway, balance_usd: str):
        """Set up mock gateway to return a single balance response."""
        mock_batch_resp = MagicMock()
        mock_balance = MagicMock()
        mock_balance.balance_usd = balance_usd
        mock_balance.error = ""
        mock_batch_resp.responses = [mock_balance]
        mock_gateway.market.BatchGetBalances.return_value = mock_batch_resp

    @pytest.mark.asyncio
    async def test_backward_compat_fields(self, executor, mock_gateway):
        """Response still includes the original fields for backward compat."""
        self._mock_balances(mock_gateway, "1000.50")
        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.status == "success"
        assert "portfolio_value_usd" in result.data
        assert "var_95" in result.data
        assert "sharpe_ratio" in result.data
        assert "volatility_annualized" in result.data
        assert "max_drawdown_pct" in result.data

    @pytest.mark.asyncio
    async def test_new_fields_present(self, executor, mock_gateway):
        """Response includes new data quality fields."""
        self._mock_balances(mock_gateway, "5000")
        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.status == "success"
        assert "data_points" in result.data
        assert "data_sufficient" in result.data
        assert "warnings" in result.data

    @pytest.mark.asyncio
    async def test_first_call_has_one_snapshot(self, executor, mock_gateway):
        """First call records one snapshot; insufficient for full metrics."""
        self._mock_balances(mock_gateway, "10000")
        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.data["data_points"] == 1
        assert result.data["data_sufficient"] is False
        assert len(result.data["warnings"]) > 0

    @pytest.mark.asyncio
    async def test_metrics_accumulate_over_calls(self, executor, mock_gateway):
        """Multiple calls accumulate snapshots and eventually compute metrics."""
        for val in ["10000", "10200", "9800", "10100"]:
            self._mock_balances(mock_gateway, val)
            result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.data["data_points"] == 4
        # With 4 snapshots, vol and Sharpe should be calculated
        assert result.data["volatility_annualized"] != "0"
        assert result.data["sharpe_ratio"] != "0"
        assert result.data["max_drawdown_pct"] != "0"

    @pytest.mark.asyncio
    async def test_portfolio_value_still_from_gateway(self, executor, mock_gateway):
        """portfolio_value_usd should reflect the latest gateway balance, not snapshots."""
        self._mock_balances(mock_gateway, "42000.75")
        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.data["portfolio_value_usd"] == "42000.75"

    @pytest.mark.asyncio
    async def test_gateway_error_still_returns_error(self, executor, mock_gateway):
        """Gateway failure path is unchanged."""
        mock_gateway.market.BatchGetBalances.side_effect = Exception("gateway unavailable")
        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.status == "error"
        assert result.error["error_code"] == "gateway_error"

    @pytest.mark.asyncio
    async def test_all_queries_fail_still_returns_error(self, executor, mock_gateway):
        """All balance query failures still produce the same error."""
        mock_batch_resp = MagicMock()
        err_resp = MagicMock()
        err_resp.balance_usd = ""
        err_resp.error = "rpc timeout"
        mock_batch_resp.responses = [err_resp, err_resp]
        mock_gateway.market.BatchGetBalances.return_value = mock_batch_resp
        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.status == "error"
        assert result.error["error_code"] == "all_queries_failed"

    @pytest.mark.asyncio
    async def test_drawdown_detected_after_portfolio_decline(self, executor, mock_gateway):
        """After a peak and decline, max_drawdown_pct is nonzero."""
        # Simulate: 10000 -> 12000 -> 9000 (25% drawdown from peak 12000)
        for val in ["10000", "12000", "9000"]:
            self._mock_balances(mock_gateway, val)
            result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        dd = Decimal(result.data["max_drawdown_pct"])
        assert dd == Decimal("0.25")


# =============================================================================
# Edge Cases
# =============================================================================


class TestRiskMetricsEdgeCases:
    def test_zero_portfolio_values(self):
        """All zero values should not cause division errors."""
        engine = _make_engine()
        _seed_snapshots(engine, [0, 0, 0, 0, 0])
        assert engine.calculate_max_drawdown() == Decimal("0")
        assert engine.calculate_volatility() == Decimal("0")
        assert engine.calculate_sharpe() == Decimal("0")
        assert engine.calculate_var_95() == Decimal("0")

    def test_portfolio_starts_at_zero(self):
        """Going from 0 to nonzero should not crash.

        Values: [0, 100, 200, 300]
        Returns: 0->100 skipped (prev=0), 100->200 = +100%, 200->300 = +50%
        Two valid returns, enough for stdev, so volatility is nonzero.
        """
        engine = _make_engine()
        _seed_snapshots(engine, [0, 100, 200, 300])
        # Should not crash; volatility is computed from the 2 valid returns
        vol = engine.calculate_volatility()
        assert vol > Decimal("0")

    def test_single_nonzero_snapshot(self):
        """Single observation can't compute any metrics."""
        engine = _make_engine()
        _seed_snapshots(engine, [1000])
        metrics = engine.get_risk_metrics()
        assert metrics["data_points"] == 1
        assert metrics["max_drawdown_pct"] == "0"
        assert metrics["volatility_annualized"] == "0"
        assert metrics["sharpe_ratio"] == "0"
        assert metrics["var_95_pct"] == "0"

    def test_very_large_values(self):
        """Large portfolio values should not overflow."""
        engine = _make_engine()
        big = Decimal("999999999999")
        _seed_snapshots(engine, [big, big * Decimal("1.01"), big * Decimal("0.99")])
        metrics = engine.get_risk_metrics()
        assert metrics["data_points"] == 3

    def test_very_small_values(self):
        """Tiny portfolio values should not cause precision issues."""
        engine = _make_engine()
        _seed_snapshots(engine, [Decimal("0.001"), Decimal("0.0011"), Decimal("0.0009")])
        metrics = engine.get_risk_metrics()
        assert metrics["data_points"] == 3

    def test_identical_adjacent_values_with_variation_elsewhere(self):
        """Some identical adjacent values mixed with variation."""
        engine = _make_engine()
        _seed_snapshots(engine, [100, 100, 110, 110, 90, 90, 100])
        vol = engine.calculate_volatility()
        # Should still compute, since there are some nonzero returns
        assert vol >= Decimal("0")

    def test_compute_returns_skips_zero_prev(self):
        """_compute_returns should skip periods where prev = 0."""
        engine = _make_engine()
        _seed_snapshots(engine, [0, 100, 200])
        returns = engine._compute_returns()
        # Only 100->200 = +100% should be present (0->100 skipped)
        assert len(returns) == 1
        assert abs(returns[0] - 1.0) < 0.0001
