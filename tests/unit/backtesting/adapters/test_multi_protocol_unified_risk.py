"""Unit tests for ``MultiProtocolBacktestAdapter.calculate_unified_risk``.

The W5 Sub-A audit flagged this method as ``GENUINELY UNCOVERED`` (CC=23, ~10%
body coverage, zero direct tests). These tests drive the post-Sub-D extracted
helpers and the orchestrator end-to-end across:

- single-protocol portfolios
- multi-protocol weighted/aggregate models
- missing-position fallbacks (no positions, no debt, no collateral)
- per-position risk classifier edges (health-factor + liquidation-price)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from almanak.framework.backtesting.adapters.multi_protocol_adapter import (
    UNIFIED_RISK_SCORE_MAX,
    AggregatedRiskResult,
    MultiProtocolBacktestAdapter,
    MultiProtocolBacktestConfig,
    UnifiedLiquidationModel,
)
from almanak.framework.backtesting.pnl.portfolio import PositionType


@dataclass
class _MockMarketState:
    """Minimal MarketState double exposing ``get_price``."""

    prices: dict[str, Decimal] = field(default_factory=dict)

    def get_price(self, token: str) -> Decimal:
        if token not in self.prices:
            raise KeyError(token)
        return self.prices[token]


def _fake_position(
    *,
    position_type: PositionType,
    health_factor: Decimal | None = None,
    liquidation_price: Decimal | None = None,
    primary_token: str = "ETH",
) -> SimpleNamespace:
    """Build a duck-typed position with only the fields the risk path reads."""
    return SimpleNamespace(
        position_type=position_type,
        health_factor=health_factor,
        liquidation_price=liquidation_price,
        primary_token=primary_token,
    )


def _fake_portfolio(*positions: Any) -> SimpleNamespace:
    return SimpleNamespace(positions=list(positions))


def _adapter_with_static_value(
    static_value: Decimal,
    *,
    model: str = "conservative",
) -> MultiProtocolBacktestAdapter:
    """Build an adapter whose ``value_position`` returns a fixed Decimal.

    Avoids dragging in a real SimulatedPortfolio + sub-adapter graph; we want
    pure logic exercised, not the valuation layer.
    """
    cfg = MultiProtocolBacktestConfig(
        strategy_type="multi_protocol",
        unified_liquidation_model=model,  # type: ignore[arg-type]
    )
    adapter = MultiProtocolBacktestAdapter(cfg)
    adapter.value_position = lambda _pos, _ms, _ts=None: static_value  # type: ignore[assignment]
    return adapter


# ---------------------------------------------------------------------------
# calculate_unified_risk — orchestrator-level cases
# ---------------------------------------------------------------------------


class TestCalculateUnifiedRiskSingleProtocol:
    """Single-protocol portfolios exercise the simple aggregation path."""

    def test_pure_supply_no_debt_returns_max_health(self) -> None:
        """Pure-collateral portfolio yields the no-debt sentinel HF and zero risk."""
        adapter = _adapter_with_static_value(Decimal("1000"))
        portfolio = _fake_portfolio(_fake_position(position_type=PositionType.SUPPLY))
        market = _MockMarketState(prices={"USDC": Decimal("1")})

        result = adapter.calculate_unified_risk(portfolio, market)

        assert isinstance(result, AggregatedRiskResult)
        assert result.unified_risk_score == Decimal("0")
        assert result.unified_health_factor == Decimal("999")  # no-debt sentinel
        assert result.total_collateral_usd == Decimal("1000")
        assert result.total_debt_usd == Decimal("0")
        assert result.at_liquidation_risk is False
        assert result.risk_model == UnifiedLiquidationModel.CONSERVATIVE
        # Single supply position groups as "lending"
        assert [exp.protocol_type for exp in result.protocol_exposures] == ["lending"]
        # Adapter records every result in history
        assert len(adapter.risk_history) == 1

class TestCalculateUnifiedRiskMultiProtocol:
    """Multi-protocol portfolios exercise the aggregator across LP / lending / perp buckets."""

    def test_weighted_model_blends_per_protocol_risk(self) -> None:
        """WEIGHTED model returns Σ(risk * value) / Σ(value) across protocols."""
        adapter = _adapter_with_static_value(Decimal("100"), model="weighted")
        # One LP (no health/liquidation hooks → risk 0) + one perp near liquidation
        lp_pos = _fake_position(position_type=PositionType.LP)
        risky_perp = _fake_position(
            position_type=PositionType.PERP_LONG,
            liquidation_price=Decimal("1900"),
            primary_token="ETH",
        )
        portfolio = _fake_portfolio(lp_pos, risky_perp)
        # Current price 2000 → distance = 100/2000 = 0.05 < 0.1 → at-risk;
        # risk = 1 - 0.05 = 0.95
        market = _MockMarketState(prices={"ETH": Decimal("2000")})

        result = adapter.calculate_unified_risk(portfolio, market)

        # Each bucket has one position valued 100. LP risk = 0, perp risk = 0.95.
        # Weighted: (0*100 + 0.95*100) / (100 + 100) = 0.475
        assert result.unified_risk_score == Decimal("0.475")
        assert result.at_liquidation_risk is True
        assert result.risk_model == UnifiedLiquidationModel.WEIGHTED
        assert result.total_collateral_usd == Decimal("200")
        assert result.total_debt_usd == Decimal("0")

    def test_aggregate_model_uses_debt_over_collateral(self) -> None:
        """AGGREGATE model returns total_debt / total_collateral."""
        adapter = _adapter_with_static_value(Decimal("100"), model="aggregate")
        supply = _fake_position(position_type=PositionType.SUPPLY)
        # Two borrows worth 100 each → debt 200, collateral 100 → ratio 2.0
        borrow_a = _fake_position(position_type=PositionType.BORROW)
        borrow_b = _fake_position(position_type=PositionType.BORROW)
        portfolio = _fake_portfolio(supply, borrow_a, borrow_b)
        market = _MockMarketState()

        result = adapter.calculate_unified_risk(portfolio, market)

        assert result.unified_risk_score == Decimal("2")
        assert result.total_collateral_usd == Decimal("100")
        assert result.total_debt_usd == Decimal("200")
        # Unified HF = 100/200 = 0.5 → below 1.0 → at_liquidation_risk
        assert result.unified_health_factor == Decimal("0.5")
        assert result.at_liquidation_risk is True

    def test_aggregate_model_debt_only_saturates_to_max_risk(self) -> None:
        """Debt-only portfolio (no collateral) is at maximum liquidation risk.

        Regression for the bug where the AGGREGATE branch returned
        ``Decimal("0")`` when ``total_debt > 0`` and ``total_collateral == 0``,
        silently understating risk. The score must saturate at the model's
        defined maximum and ``at_liquidation_risk`` must be True.
        """
        adapter = _adapter_with_static_value(Decimal("100"), model="aggregate")
        borrow = _fake_position(position_type=PositionType.BORROW)
        portfolio = _fake_portfolio(borrow)
        market = _MockMarketState()

        result = adapter.calculate_unified_risk(portfolio, market)

        assert result.total_collateral_usd == Decimal("0")
        assert result.total_debt_usd == Decimal("100")
        assert result.unified_risk_score == UNIFIED_RISK_SCORE_MAX
        assert result.at_liquidation_risk is True
        assert result.risk_model == UnifiedLiquidationModel.AGGREGATE


class TestCalculateUnifiedRiskFallbacks:
    """Empty / boundary portfolios should not crash and should return safe sentinels."""

    def test_empty_portfolio_returns_zero_risk_and_max_hf(self) -> None:
        """An empty portfolio has zero exposure and the no-debt HF sentinel."""
        adapter = _adapter_with_static_value(Decimal("0"))
        portfolio = _fake_portfolio()
        market = _MockMarketState()

        result = adapter.calculate_unified_risk(portfolio, market)

        assert result.protocol_exposures == []
        assert result.total_collateral_usd == Decimal("0")
        assert result.total_debt_usd == Decimal("0")
        assert result.unified_risk_score == Decimal("0")
        assert result.unified_health_factor == Decimal("999")
        assert result.at_liquidation_risk is False


# ---------------------------------------------------------------------------
# Per-helper edge cases (extracted classifier / aggregator)
# ---------------------------------------------------------------------------


class TestEvaluatePositionRisk:
    """Direct tests for the extracted per-position classifier."""

    def test_missing_price_for_perp_falls_back_silently(self) -> None:
        """A KeyError from ``market_state.get_price`` is treated as 'no risk signal'."""
        perp = _fake_position(
            position_type=PositionType.PERP_LONG,
            liquidation_price=Decimal("1500"),
            primary_token="UNKNOWN",
        )
        empty_market = _MockMarketState()  # raises KeyError on any token

        risk, at_risk = MultiProtocolBacktestAdapter._evaluate_position_risk(perp, empty_market)

        assert risk == Decimal("0")
        assert at_risk is False

