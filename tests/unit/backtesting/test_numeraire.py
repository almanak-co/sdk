"""Unit tests for the numeraire reporting projection (VIB-5127).

Covers the shared ``almanak.framework.backtesting.numeraire`` module:
strategy -> symbol resolution (USD default, token kind, chain-mismatch
fail-loud), the equity-series projection + metric computation for both engine
conventions, the unpriceable fail-loud, and the additive serialization
round-trip on the shared models.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
    NumeraireMetrics,
)
from almanak.framework.backtesting.numeraire import (
    compute_numeraire_metrics,
    compute_numeraire_metrics_paper,
    resolve_numeraire_symbol,
)

# WETH on Arbitrum (the default backtest chain in the trust matrix).
WETH_ARBITRUM = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
_TS0 = datetime(2024, 1, 1, tzinfo=UTC)


def _curve(prices: list[Decimal | None], value_usd: Decimal = Decimal("10000")) -> list[EquityPoint]:
    return [
        EquityPoint(timestamp=_TS0 + timedelta(hours=i), value_usd=value_usd, numeraire_price_usd=p)
        for i, p in enumerate(prices)
    ]


class _Strategy:
    def __init__(self, quote_asset: object | None = None) -> None:
        if quote_asset is not None:
            self.quote_asset = quote_asset


# --- resolve_numeraire_symbol --------------------------------------------------


def test_resolve_usd_strategy_returns_none() -> None:
    assert resolve_numeraire_symbol(_Strategy(QuoteAsset.usd()), "arbitrum") is None


def test_resolve_strategy_without_quote_asset_attr_returns_none() -> None:
    # The backtest duck type only requires deployment_id + decide; a strategy
    # exposing no quote_asset must read as USD, not raise.
    assert resolve_numeraire_symbol(_Strategy(), "arbitrum") is None


def test_resolve_token_on_backtest_chain_returns_symbol() -> None:
    strategy = _Strategy(QuoteAsset.token(42161, WETH_ARBITRUM))
    assert resolve_numeraire_symbol(strategy, "arbitrum") == "WETH"


def test_resolve_token_chain_mismatch_raises() -> None:
    strategy = _Strategy(QuoteAsset.token(42161, WETH_ARBITRUM))  # Arbitrum token
    with pytest.raises(ValueError, match="must live on the backtest chain"):
        resolve_numeraire_symbol(strategy, "base")


# --- compute_numeraire_metrics (PnL / daily convention) ------------------------


def test_compute_usd_symbol_returns_none_triplet() -> None:
    out = compute_numeraire_metrics(
        _curve([Decimal("2000")] * 3),
        numeraire_symbol=None,
        trading_days_per_year=365,
        risk_free_rate=Decimal("0"),
    )
    assert out == (None, None, None)


def test_compute_empty_curve_returns_none_triplet() -> None:
    out = compute_numeraire_metrics(
        [],
        numeraire_symbol="WETH",
        trading_days_per_year=365,
        risk_free_rate=Decimal("0"),
    )
    assert out == (None, None, None)


def test_compute_flat_price_conserves_exactly() -> None:
    metrics, initial, final = compute_numeraire_metrics(
        _curve([Decimal("2000")] * 8),
        numeraire_symbol="WETH",
        trading_days_per_year=365,
        risk_free_rate=Decimal("0"),
    )
    assert initial == Decimal("5")  # 10000 / 2000
    assert final == Decimal("5")
    assert metrics is not None
    assert metrics.numeraire == "WETH"
    assert metrics.total_pnl == Decimal("0")
    assert metrics.total_return_pct == Decimal("0")
    assert metrics.max_drawdown_pct == Decimal("0")


def test_compute_numeraire_appreciation_shows_loss_in_numeraire() -> None:
    # Portfolio flat in USD (10,000) but WETH doubles 2000 -> 4000: in WETH the
    # portfolio HALVES (5 -> 2.5 WETH). The numeraire view exposes that the USD
    # edge was just the quote asset's own move.
    prices = [Decimal("2000")] * 4 + [Decimal("4000")] * 4
    metrics, initial, final = compute_numeraire_metrics(
        _curve(prices),
        numeraire_symbol="WETH",
        trading_days_per_year=365,
        risk_free_rate=Decimal("0"),
    )
    assert initial == Decimal("5")
    assert final == Decimal("2.5")
    assert metrics is not None
    assert metrics.total_pnl == Decimal("-2.5")
    assert metrics.total_return_pct == Decimal("-50")


def test_compute_unpriceable_none_raises() -> None:
    with pytest.raises(ValueError, match="unpriceable"):
        compute_numeraire_metrics(
            _curve([Decimal("2000"), None, Decimal("2000")]),
            numeraire_symbol="WETH",
            trading_days_per_year=365,
            risk_free_rate=Decimal("0"),
        )


def test_compute_zero_price_raises() -> None:
    with pytest.raises(ValueError, match="unpriceable"):
        compute_numeraire_metrics(
            _curve([Decimal("2000"), Decimal("0")]),
            numeraire_symbol="WETH",
            trading_days_per_year=365,
            risk_free_rate=Decimal("0"),
        )


# --- compute_numeraire_metrics_paper (hourly convention) -----------------------


def test_paper_variant_flat_price_conserves_and_omits_daily_only_fields() -> None:
    metrics, initial, final = compute_numeraire_metrics_paper(
        _curve([Decimal("2500")] * 6),
        numeraire_symbol="WETH",
    )
    assert initial == Decimal("4")  # 10000 / 2500
    assert final == Decimal("4")
    assert metrics is not None
    # Paper leaves sortino / calmar / annualized at 0, matching its USD metrics.
    assert metrics.sortino_ratio == Decimal("0")
    assert metrics.calmar_ratio == Decimal("0")
    assert metrics.annualized_return_pct == Decimal("0")


def test_paper_variant_usd_symbol_returns_none_triplet() -> None:
    assert compute_numeraire_metrics_paper(_curve([Decimal("2000")]), numeraire_symbol=None) == (None, None, None)


def test_paper_variant_unpriceable_raises() -> None:
    with pytest.raises(ValueError, match="unpriceable"):
        compute_numeraire_metrics_paper(_curve([None]), numeraire_symbol="WETH")


# --- additive serialization round-trip -----------------------------------------


def test_usd_result_serializes_without_numeraire_keys() -> None:
    result = BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="d",
        start_time=_TS0,
        end_time=_TS0 + timedelta(hours=1),
        metrics=BacktestMetrics(),
        equity_curve=_curve([None]),
    )
    payload = result.to_dict()
    assert "numeraire" not in payload
    assert "initial_capital_numeraire" not in payload
    assert "final_capital_numeraire" not in payload
    assert "numeraire_metrics" not in payload["metrics"]
    assert all("numeraire_price_usd" not in point for point in payload["equity_curve"])
    # round-trips back to a USD artifact
    assert BacktestResult.from_dict(payload).numeraire is None


def test_token_result_round_trips_numeraire_fields() -> None:
    nm = NumeraireMetrics(numeraire="WETH", total_pnl=Decimal("0.5"), total_return_pct=Decimal("10"))
    result = BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="d",
        start_time=_TS0,
        end_time=_TS0 + timedelta(hours=1),
        metrics=BacktestMetrics(total_pnl_usd=Decimal("1000"), numeraire_metrics=nm),
        equity_curve=_curve([Decimal("2000")]),
        numeraire="WETH",
        initial_capital_numeraire=Decimal("5"),
        final_capital_numeraire=Decimal("5.5"),
    )
    restored = BacktestResult.from_dict(result.to_dict())
    assert restored.numeraire == "WETH"
    assert restored.initial_capital_numeraire == Decimal("5")
    assert restored.final_capital_numeraire == Decimal("5.5")
    assert restored.equity_curve[0].numeraire_price_usd == Decimal("2000")
    assert restored.metrics.numeraire_metrics is not None
    assert restored.metrics.numeraire_metrics.numeraire == "WETH"
    assert restored.metrics.numeraire_metrics.total_return_pct == Decimal("10")


def test_simulated_portfolio_round_trips_numeraire_context() -> None:
    # A resumed non-USD PnL run must keep its numeraire symbol and the captured
    # per-point numeraire price (VIB-5127).
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    portfolio._numeraire_symbol = "WETH"
    portfolio.equity_curve.append(
        EquityPoint(timestamp=_TS0, value_usd=Decimal("10000"), numeraire_price_usd=Decimal("2000"))
    )

    restored = SimulatedPortfolio.from_dict(portfolio.to_dict())
    assert restored._numeraire_symbol == "WETH"
    assert restored.equity_curve[0].numeraire_price_usd == Decimal("2000")

    # USD portfolio: numeraire context round-trips as None.
    usd = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    usd.equity_curve.append(EquityPoint(timestamp=_TS0, value_usd=Decimal("10000")))
    usd_restored = SimulatedPortfolio.from_dict(usd.to_dict())
    assert usd_restored._numeraire_symbol is None
    assert usd_restored.equity_curve[0].numeraire_price_usd is None
