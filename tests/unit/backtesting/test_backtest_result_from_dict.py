"""Characterization tests for `BacktestResult.from_dict` serialization.

These tests lock in the current behaviour of `BacktestResult.from_dict` before
refactoring the method into smaller helpers. They cover:

- v1 -> v2 metrics schema migration (percentage vs ratio semantics)
- v2 native rehydration (no migration)
- Optional fields (None / default handling)
- Malformed / partial input (missing top-level keys)
- Nested dataclass rehydration (equity_curve, trades, lending_liquidations,
  reconciliation_events, walk_forward_results, monte_carlo_results,
  crisis_results, data_quality, preflight_report, gas_prices_used,
  parameter_sources, accuracy_estimate, data_coverage_metrics,
  aggregated_portfolio_view)
- Full round-trip (to_dict -> from_dict) stability for a populated result

These tests are intentionally dict-shape focused (not Python-object focused) so
that on-disk artifacts produced by earlier SDK versions can still be loaded by
CLI pnl, dashboard, and strategy-tester code after the refactor. Do NOT relax
the assertions without explicitly considering artifact backward compatibility.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import (
    AggregatedPortfolioView,
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    CrisisMetrics,
    EquityPoint,
    GasPriceRecord,
    IntentType,
    LendingLiquidationEvent,
    ReconciliationEvent,
    TradeRecord,
)

# -----------------------------------------------------------------------------
# Fixture helpers
# -----------------------------------------------------------------------------


def _minimal_dict(**overrides: Any) -> dict[str, Any]:
    """Build a minimal valid serialized BacktestResult dict.

    Mirrors the smallest shape that `from_dict` is expected to accept in
    production (engine + strategy_id + start/end time + metrics + trades +
    equity_curve). Additional fields can be added via kwargs.
    """
    base: dict[str, Any] = {
        "engine": "pnl",
        "strategy_id": "test_strategy",
        "start_time": "2024-01-01T00:00:00",
        "end_time": "2024-01-31T00:00:00",
        "metrics": {},
        "trades": [],
        "equity_curve": [],
    }
    base.update(overrides)
    return base


# -----------------------------------------------------------------------------
# v1 schema migration (legacy metrics, total_return_pct / annualized as ratio)
# -----------------------------------------------------------------------------


class TestV1SchemaMigration:
    """VIB-2915: v1 stored return pct as ratio (0.10 == 10%)."""

    def test_v1_metrics_missing_schema_version_is_treated_as_v1(self) -> None:
        """A metrics dict with no schema_version must be migrated (ratio -> percent)."""
        data = _minimal_dict(
            metrics={
                # no schema_version key -> defaults to 1 -> migrate
                "total_return_pct": "0.10",
                "annualized_return_pct": "0.20",
            }
        )

        result = BacktestResult.from_dict(data)

        assert result.metrics.total_return_pct == Decimal("10")
        assert result.metrics.annualized_return_pct == Decimal("20")

    def test_v1_explicit_schema_version_migrates(self) -> None:
        """An explicit schema_version=1 must be migrated identically."""
        data = _minimal_dict(
            metrics={
                "schema_version": 1,
                "total_return_pct": "0.25",
                "annualized_return_pct": "0.50",
            }
        )

        result = BacktestResult.from_dict(data)

        assert result.metrics.total_return_pct == Decimal("25")
        assert result.metrics.annualized_return_pct == Decimal("50")

    def test_v1_zero_returns_stay_zero(self) -> None:
        """Migration must not introduce non-zero values from zeros."""
        data = _minimal_dict(
            metrics={
                "total_return_pct": "0",
                "annualized_return_pct": "0",
            }
        )

        result = BacktestResult.from_dict(data)

        assert result.metrics.total_return_pct == Decimal("0")
        assert result.metrics.annualized_return_pct == Decimal("0")

    def test_v1_negative_returns_migrate(self) -> None:
        """Negative ratios must migrate to negative percentages."""
        data = _minimal_dict(
            metrics={
                "total_return_pct": "-0.15",
                "annualized_return_pct": "-0.30",
            }
        )

        result = BacktestResult.from_dict(data)

        assert result.metrics.total_return_pct == Decimal("-15.00")
        assert result.metrics.annualized_return_pct == Decimal("-30.00")


# -----------------------------------------------------------------------------
# v2 schema (current)
# -----------------------------------------------------------------------------


class TestV2SchemaRehydration:
    """Current schema: returns are whole percentages (10 == 10%)."""

    def test_v2_metrics_are_not_migrated(self) -> None:
        """schema_version == SCHEMA_VERSION must leave percentage values as-is."""
        data = _minimal_dict(
            metrics={
                "schema_version": BacktestMetrics.SCHEMA_VERSION,
                "total_return_pct": "10",
                "annualized_return_pct": "20",
            }
        )

        result = BacktestResult.from_dict(data)

        assert result.metrics.total_return_pct == Decimal("10")
        assert result.metrics.annualized_return_pct == Decimal("20")

    def test_v2_preserves_all_scalar_metrics(self) -> None:
        """All populated scalar metric fields should be rehydrated as Decimal/int."""
        data = _minimal_dict(
            metrics={
                "schema_version": 2,
                "total_pnl_usd": "1234.56",
                "net_pnl_usd": "1000.00",
                "sharpe_ratio": "1.5",
                "max_drawdown_pct": "0.12",
                "win_rate": "0.6",
                "total_trades": 42,
                "profit_factor": "2.1",
                "total_return_pct": "15",
                "annualized_return_pct": "25",
                "winning_trades": 25,
                "losing_trades": 17,
                "min_health_factor": "1.8",
                "health_factor_warnings": 3,
            }
        )

        result = BacktestResult.from_dict(data)

        assert result.metrics.total_pnl_usd == Decimal("1234.56")
        assert result.metrics.net_pnl_usd == Decimal("1000.00")
        assert result.metrics.sharpe_ratio == Decimal("1.5")
        assert result.metrics.total_trades == 42
        assert result.metrics.winning_trades == 25
        assert result.metrics.losing_trades == 17
        assert result.metrics.min_health_factor == Decimal("1.8")
        assert result.metrics.health_factor_warnings == 3


# -----------------------------------------------------------------------------
# Optional / default field handling
# -----------------------------------------------------------------------------


class TestOptionalFieldHandling:
    """Fields that are Optional in the dataclass must round-trip None correctly."""

    def test_minimal_dict_produces_valid_result_with_defaults(self) -> None:
        """A minimal dict must produce a BacktestResult with documented defaults."""
        result = BacktestResult.from_dict(_minimal_dict())

        assert result.engine == BacktestEngine.PNL
        assert result.strategy_id == "test_strategy"
        assert result.initial_capital_usd == Decimal("10000")
        assert result.final_capital_usd == Decimal("10000")
        assert result.chain == "arbitrum"
        assert result.run_started_at is None
        assert result.run_ended_at is None
        assert result.run_duration_seconds == 0.0
        assert result.config == {}
        assert result.error is None
        assert result.backtest_id is None
        assert result.config_hash is None
        assert result.execution_delayed_at_end == 0
        assert result.institutional_compliance is True
        assert result.preflight_passed is True
        # Optional dataclass references default to None:
        assert result.aggregated_portfolio_view is None
        assert result.walk_forward_results is None
        assert result.monte_carlo_results is None
        assert result.crisis_results is None
        assert result.data_quality is None
        assert result.preflight_report is None
        assert result.gas_price_summary is None
        assert result.parameter_sources is None
        assert result.accuracy_estimate is None
        assert result.data_coverage_metrics is None
        # Optional metric scalars that default to None
        assert result.metrics.correlation_risk is None
        assert result.metrics.information_ratio is None
        assert result.metrics.beta is None
        assert result.metrics.alpha is None
        assert result.metrics.benchmark_return is None

    def test_explicit_nones_for_optional_subobjects(self) -> None:
        """Explicit None values for optional subobjects must round-trip as None."""
        data = _minimal_dict(
            aggregated_portfolio_view=None,
            walk_forward_results=None,
            monte_carlo_results=None,
            crisis_results=None,
            data_quality=None,
            preflight_report=None,
            gas_price_summary=None,
            parameter_sources=None,
            accuracy_estimate=None,
            data_coverage_metrics=None,
        )

        result = BacktestResult.from_dict(data)

        assert result.aggregated_portfolio_view is None
        assert result.walk_forward_results is None
        assert result.monte_carlo_results is None
        assert result.crisis_results is None
        assert result.data_quality is None
        assert result.preflight_report is None
        assert result.gas_price_summary is None
        assert result.parameter_sources is None
        assert result.accuracy_estimate is None
        assert result.data_coverage_metrics is None

    def test_optional_metric_decimals_preserved_when_present(self) -> None:
        """Optional Decimal metrics (beta, alpha, etc.) must parse when present."""
        data = _minimal_dict(
            metrics={
                "schema_version": 2,
                "correlation_risk": "0.42",
                "information_ratio": "1.1",
                "beta": "0.9",
                "alpha": "0.02",
                "benchmark_return": "0.08",
            }
        )

        result = BacktestResult.from_dict(data)

        assert result.metrics.correlation_risk == Decimal("0.42")
        assert result.metrics.information_ratio == Decimal("1.1")
        assert result.metrics.beta == Decimal("0.9")
        assert result.metrics.alpha == Decimal("0.02")
        assert result.metrics.benchmark_return == Decimal("0.08")


# -----------------------------------------------------------------------------
# Malformed / partial input
# -----------------------------------------------------------------------------


class TestMalformedInput:
    """from_dict should fail loudly on malformed required fields, not silently coerce."""

    def test_missing_engine_raises_keyerror(self) -> None:
        data = _minimal_dict()
        del data["engine"]
        with pytest.raises(KeyError):
            BacktestResult.from_dict(data)

    def test_invalid_engine_value_raises_valueerror(self) -> None:
        data = _minimal_dict(engine="not_a_real_engine")
        with pytest.raises(ValueError):
            BacktestResult.from_dict(data)

    def test_missing_strategy_id_raises_keyerror(self) -> None:
        data = _minimal_dict()
        del data["strategy_id"]
        with pytest.raises(KeyError):
            BacktestResult.from_dict(data)

    def test_missing_start_time_raises_keyerror(self) -> None:
        data = _minimal_dict()
        del data["start_time"]
        with pytest.raises(KeyError):
            BacktestResult.from_dict(data)

    def test_malformed_start_time_raises_valueerror(self) -> None:
        data = _minimal_dict(start_time="not-a-datetime")
        with pytest.raises(ValueError):
            BacktestResult.from_dict(data)

    def test_missing_metrics_defaults_to_empty_dict(self) -> None:
        """Absent metrics field should default to an empty BacktestMetrics."""
        data = _minimal_dict()
        del data["metrics"]
        result = BacktestResult.from_dict(data)
        # Empty metrics dict -> defaults (treated as v1 due to missing
        # schema_version; 0 * 100 still equals 0)
        assert result.metrics.total_pnl_usd == Decimal("0")
        assert result.metrics.total_return_pct == Decimal("0")

    def test_explicit_none_for_collections_is_tolerated(self) -> None:
        """JSON `null` on top-level collection fields should behave like absent.

        Defensive behaviour -- `to_dict` never emits explicit None for these
        fields, but hand-authored / third-party artifacts may. Treating
        explicit None as "empty" avoids a TypeError at the iteration site and
        matches the absent-key default.
        """
        data = _minimal_dict(metrics=None, trades=None, equity_curve=None)
        result = BacktestResult.from_dict(data)
        assert result.trades == []
        assert result.equity_curve == []
        assert result.metrics.total_pnl_usd == Decimal("0")


# -----------------------------------------------------------------------------
# Nested dataclass rehydration
# -----------------------------------------------------------------------------


class TestNestedDataclassRehydration:
    """Every nested dataclass field must be correctly typed after from_dict."""

    def test_equity_curve_rehydrates_as_equity_points(self) -> None:
        data = _minimal_dict(
            equity_curve=[
                {
                    "timestamp": "2024-01-01T00:00:00",
                    "value_usd": "10000",
                    "eth_price_usd": "2500",
                    "spot_value_usd": "5000",
                    "position_value_usd": "5000",
                    "valuation_source": "full",
                },
                {
                    "timestamp": "2024-01-02T00:00:00",
                    "value_usd": "10100",
                    # no optional fields on this point
                },
            ]
        )

        result = BacktestResult.from_dict(data)

        assert len(result.equity_curve) == 2
        assert all(isinstance(p, EquityPoint) for p in result.equity_curve)
        assert result.equity_curve[0].value_usd == Decimal("10000")
        assert result.equity_curve[0].eth_price_usd == Decimal("2500")
        assert result.equity_curve[0].valuation_source == "full"
        assert result.equity_curve[1].eth_price_usd is None
        assert result.equity_curve[1].valuation_source == "simple"

    def test_equity_point_tolerates_explicit_none_optionals(self) -> None:
        """Explicit JSON `null` on EquityPoint optional fields must round-trip to None.

        `to_dict` only emits these keys when non-None, so this is a defensive
        check against hand-authored / third-party artifacts.
        """
        data = _minimal_dict(
            equity_curve=[
                {
                    "timestamp": "2024-01-01T00:00:00",
                    "value_usd": "10000",
                    "eth_price_usd": None,
                    "spot_value_usd": None,
                    "position_value_usd": None,
                    "valuation_source": None,
                },
            ]
        )

        result = BacktestResult.from_dict(data)

        assert len(result.equity_curve) == 1
        assert result.equity_curve[0].eth_price_usd is None
        assert result.equity_curve[0].spot_value_usd is None
        assert result.equity_curve[0].position_value_usd is None
        # valuation_source is a required str field with a "simple" default;
        # explicit null should fall back to the default.
        assert result.equity_curve[0].valuation_source == "simple"

    def test_trades_rehydrate_as_trade_records(self) -> None:
        data = _minimal_dict(
            trades=[
                {
                    "timestamp": "2024-01-01T10:00:00",
                    "intent_type": IntentType.SWAP.value,
                    "executed_price": "2500.50",
                    "fee_usd": "1.25",
                    "slippage_usd": "0.50",
                    "gas_cost_usd": "2.00",
                    "pnl_usd": "100.00",
                    "success": True,
                    "amount_usd": "1000.00",
                    "protocol": "uniswap_v3",
                    "tokens": ["ETH", "USDC"],
                    "tx_hash": "0xabc",
                    "metadata": {"pool": "ETH/USDC"},
                    "delayed_at_end": False,
                    "position_id": None,
                }
            ]
        )

        result = BacktestResult.from_dict(data)

        assert len(result.trades) == 1
        trade = result.trades[0]
        assert isinstance(trade, TradeRecord)
        assert trade.intent_type == IntentType.SWAP
        assert trade.executed_price == Decimal("2500.50")
        assert trade.amount_usd == Decimal("1000.00")
        assert trade.protocol == "uniswap_v3"
        assert trade.tokens == ["ETH", "USDC"]
        assert trade.metadata == {"pool": "ETH/USDC"}
        assert trade.actual_amount_in is None
        assert trade.actual_amount_out is None
        assert trade.delayed_at_end is False

    def test_trades_rehydrate_optional_decimals_when_present(self) -> None:
        data = _minimal_dict(
            trades=[
                {
                    "timestamp": "2024-01-01T10:00:00",
                    "intent_type": IntentType.LP_OPEN.value,
                    "executed_price": "2500.50",
                    "fee_usd": "0",
                    "slippage_usd": "0",
                    "gas_cost_usd": "0",
                    "pnl_usd": "0",
                    "success": True,
                    "actual_amount_in": "1.5",
                    "actual_amount_out": "3000",
                    "expected_amount_in": "1.5",
                    "expected_amount_out": "3000",
                    "il_loss_usd": "-5.0",
                    "fees_earned_usd": "10.0",
                    "net_lp_pnl_usd": "5.0",
                    "gas_price_gwei": "30",
                    "estimated_mev_cost_usd": "0.25",
                }
            ]
        )

        result = BacktestResult.from_dict(data)

        trade = result.trades[0]
        assert trade.actual_amount_in == Decimal("1.5")
        assert trade.actual_amount_out == Decimal("3000")
        assert trade.expected_amount_in == Decimal("1.5")
        assert trade.expected_amount_out == Decimal("3000")
        assert trade.il_loss_usd == Decimal("-5.0")
        assert trade.fees_earned_usd == Decimal("10.0")
        assert trade.net_lp_pnl_usd == Decimal("5.0")
        assert trade.gas_price_gwei == Decimal("30")
        assert trade.estimated_mev_cost_usd == Decimal("0.25")

    def test_aggregated_portfolio_view_is_rehydrated(self) -> None:
        data = _minimal_dict(
            aggregated_portfolio_view={
                "snapshots": [{"timestamp": "2024-01-01T00:00:00"}],
                "final_risk_score": "0.25",
                "max_risk_score": "0.40",
                "avg_risk_score": "0.15",
                "risk_score_history": [{"timestamp": "2024-01-01T00:00:00", "score": "0.15"}],
            }
        )

        result = BacktestResult.from_dict(data)

        assert isinstance(result.aggregated_portfolio_view, AggregatedPortfolioView)
        assert result.aggregated_portfolio_view.final_risk_score == Decimal("0.25")
        assert result.aggregated_portfolio_view.max_risk_score == Decimal("0.40")
        assert len(result.aggregated_portfolio_view.snapshots) == 1

    def test_crisis_results_rehydrates(self) -> None:
        data = _minimal_dict(
            crisis_results={
                "scenario_name": "ftx_collapse",
                "scenario_start": "2022-11-06T00:00:00",
                "scenario_end": "2022-11-14T00:00:00",
                "scenario_duration_days": 8,
                "max_drawdown_pct": "0.22",
                "drawdown_start": None,
                "drawdown_trough": None,
                "days_to_trough": 3,
                "recovery_time_days": None,
                "recovery_pct": "0",
                "total_return_pct": "-0.15",
                "volatility": "0.40",
                "sharpe_ratio": "-0.5",
                "total_trades": 10,
                "winning_trades": 3,
                "losing_trades": 7,
                "win_rate": "0.3",
                "total_costs_usd": "50",
                "normal_period_comparison": {},
            }
        )

        result = BacktestResult.from_dict(data)

        assert isinstance(result.crisis_results, CrisisMetrics)
        assert result.crisis_results.scenario_name == "ftx_collapse"
        assert result.crisis_results.max_drawdown_pct == Decimal("0.22")

    def test_data_source_capabilities_rehydrates_enum_values(self) -> None:
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability

        data = _minimal_dict(
            data_source_capabilities={
                "chainlink": HistoricalDataCapability.FULL.value,
                "coingecko": HistoricalDataCapability.CURRENT_ONLY.value,
            }
        )

        result = BacktestResult.from_dict(data)

        assert result.data_source_capabilities["chainlink"] == HistoricalDataCapability.FULL
        assert result.data_source_capabilities["coingecko"] == HistoricalDataCapability.CURRENT_ONLY

    def test_gas_prices_used_rehydrates_as_records(self) -> None:
        data = _minimal_dict(
            gas_prices_used=[
                {
                    "timestamp": "2024-01-01T00:00:00",
                    "gwei": "0.1",
                    "source": "market_state",
                    "usd_cost": "0.05",
                    "eth_price_usd": "2500",
                }
            ]
        )

        result = BacktestResult.from_dict(data)

        assert len(result.gas_prices_used) == 1
        assert isinstance(result.gas_prices_used[0], GasPriceRecord)
        assert result.gas_prices_used[0].gwei == Decimal("0.1")
        assert result.gas_prices_used[0].source == "market_state"

    def test_lending_liquidations_rehydrate(self) -> None:
        data = _minimal_dict(
            lending_liquidations=[
                {
                    "timestamp": "2024-01-15T12:00:00",
                    "position_id": "pos-123",
                    "health_factor": "0.95",
                    "collateral_seized": "550.00",
                    "debt_repaid": "500.00",
                    "penalty": "0.05",
                }
            ]
        )

        result = BacktestResult.from_dict(data)

        assert len(result.lending_liquidations) == 1
        assert isinstance(result.lending_liquidations[0], LendingLiquidationEvent)
        assert result.lending_liquidations[0].position_id == "pos-123"
        assert result.lending_liquidations[0].health_factor == Decimal("0.95")

    def test_reconciliation_events_rehydrate(self) -> None:
        data = _minimal_dict(
            reconciliation_events=[
                {
                    "timestamp": "2024-01-15T12:00:00",
                    "position_id": "pos-1",
                    "expected": "100",
                    "actual": "95",
                    "discrepancy": "5",
                    "discrepancy_pct": "5",
                    "field_name": "amount",
                    "auto_corrected": True,
                }
            ]
        )

        result = BacktestResult.from_dict(data)

        assert len(result.reconciliation_events) == 1
        assert isinstance(result.reconciliation_events[0], ReconciliationEvent)
        assert result.reconciliation_events[0].position_id == "pos-1"
        assert result.reconciliation_events[0].auto_corrected is True


# -----------------------------------------------------------------------------
# Full round-trip stability
# -----------------------------------------------------------------------------


class TestRoundTripStability:
    """Serialized -> from_dict -> to_dict should be idempotent after the first load."""

    def _make_populated_result(self) -> BacktestResult:
        return BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="round_trip_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(
                total_pnl_usd=Decimal("123.45"),
                total_return_pct=Decimal("10"),
                annualized_return_pct=Decimal("20"),
                total_trades=5,
                winning_trades=3,
                losing_trades=2,
                correlation_risk=Decimal("0.5"),
            ),
            trades=[
                TradeRecord(
                    timestamp=datetime(2024, 1, 2, 10, 30),
                    intent_type=IntentType.SWAP,
                    executed_price=Decimal("2500"),
                    fee_usd=Decimal("1"),
                    slippage_usd=Decimal("0.25"),
                    gas_cost_usd=Decimal("2"),
                    pnl_usd=Decimal("15"),
                    success=True,
                    amount_usd=Decimal("1000"),
                    protocol="uniswap_v3",
                    tokens=["ETH", "USDC"],
                )
            ],
            equity_curve=[
                EquityPoint(
                    timestamp=datetime(2024, 1, 1),
                    value_usd=Decimal("10000"),
                    valuation_source="simple",
                ),
                EquityPoint(
                    timestamp=datetime(2024, 1, 31),
                    value_usd=Decimal("11000"),
                    eth_price_usd=Decimal("2500"),
                    valuation_source="full",
                ),
            ],
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("11000"),
            chain="arbitrum",
            run_started_at=datetime(2024, 2, 1, 0, 0, 0),
            run_ended_at=datetime(2024, 2, 1, 0, 5, 0),
            run_duration_seconds=300.0,
            config={"ticker": "ETH"},
            errors=[{"error_type": "Test", "error_message": "Test message"}],
            backtest_id="bt-abc123",
            config_hash="deadbeef",
            institutional_compliance=True,
            preflight_passed=True,
        )

    def test_from_dict_of_to_dict_preserves_shape(self) -> None:
        """to_dict -> from_dict must preserve all populated fields."""
        original = self._make_populated_result()
        restored = BacktestResult.from_dict(original.to_dict())

        assert restored.engine == original.engine
        assert restored.strategy_id == original.strategy_id
        assert restored.start_time == original.start_time
        assert restored.end_time == original.end_time
        assert restored.initial_capital_usd == original.initial_capital_usd
        assert restored.final_capital_usd == original.final_capital_usd
        assert restored.chain == original.chain
        assert restored.run_started_at == original.run_started_at
        assert restored.run_ended_at == original.run_ended_at
        assert restored.run_duration_seconds == original.run_duration_seconds
        assert restored.config == original.config
        assert restored.backtest_id == original.backtest_id
        assert restored.config_hash == original.config_hash
        assert restored.institutional_compliance == original.institutional_compliance
        assert restored.preflight_passed == original.preflight_passed
        # Metrics
        assert restored.metrics.total_pnl_usd == original.metrics.total_pnl_usd
        assert restored.metrics.total_return_pct == original.metrics.total_return_pct
        assert restored.metrics.correlation_risk == original.metrics.correlation_risk
        # Nested
        assert len(restored.trades) == 1
        assert restored.trades[0].intent_type == IntentType.SWAP
        assert restored.trades[0].executed_price == Decimal("2500")
        assert len(restored.equity_curve) == 2
        assert restored.equity_curve[1].eth_price_usd == Decimal("2500")

    def test_second_round_trip_is_identical_to_first(self) -> None:
        """Once through `from_dict`, further `to_dict -> from_dict` is stable.

        This locks in idempotence: the migrated v2 representation must not
        re-migrate or otherwise drift on successive serializations.
        """
        original = self._make_populated_result()
        first = BacktestResult.from_dict(original.to_dict())
        second = BacktestResult.from_dict(first.to_dict())

        assert first.to_dict() == second.to_dict()
