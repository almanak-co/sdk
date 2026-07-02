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
    numeraire_token_address,
    resolve_numeraire_symbol,
)

# WETH on Arbitrum (the default backtest chain in the trust matrix).
WETH_ARBITRUM = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
# cbBTC on Base (chain_id 8453) -- a non-native ERC20 absent from the native
# CoinGecko-id projection, so it is only priceable by contract address. This is
# the exact numeraire from the reported unpriceable-numeraire bug.
CBBTC_BASE = "0xBdb9300b7CDE636d9cD4AFF00f6F009fFBBc8EE6"
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


# --- numeraire_token_address ---------------------------------------------------


def test_numeraire_address_usd_strategy_returns_none() -> None:
    assert numeraire_token_address(_Strategy(QuoteAsset.usd()), "base") is None


def test_numeraire_address_no_quote_asset_attr_returns_none() -> None:
    # A strategy exposing no quote_asset reads as USD -> no address to register.
    assert numeraire_token_address(_Strategy(), "base") is None


def test_numeraire_address_token_returns_chain_and_address() -> None:
    # The address comes straight from the QuoteAsset (canonical (chain_id,
    # address)); QuoteAsset lower-cases EVM addresses as its canonical key.
    strategy = _Strategy(QuoteAsset.token(8453, CBBTC_BASE))
    assert numeraire_token_address(strategy, "base") == ("base", CBBTC_BASE.lower())


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


# ---------------------------------------------------------------------------
# Numeraire-canonical merge (blueprint 31 §7)
# ---------------------------------------------------------------------------


def _moving_price_fixture() -> tuple[list[EquityPoint], list["TradeRecord"], NumeraireMetrics]:
    """Buy-low / sell-high fixture with a moving numeraire price.

    Curve: 10,000 USD flat, jumps to 11,250 USD when the sell realizes
    +1,250 gross at t2; WETH price moves 2,000 -> 2,500 at t2. In WETH the
    portfolio decays 5 -> 4.5 despite the positive USD delta.
    """
    from almanak.framework.backtesting.models import IntentType, TradeRecord

    prices = [Decimal("2000"), Decimal("2000"), Decimal("2500"), Decimal("2500")]
    values = [Decimal("10000"), Decimal("10000"), Decimal("11250"), Decimal("11250")]
    curve = [
        EquityPoint(timestamp=_TS0 + timedelta(hours=i), value_usd=v, numeraire_price_usd=p)
        for i, (v, p) in enumerate(zip(values, prices, strict=True))
    ]
    trades = [
        TradeRecord(
            timestamp=_TS0 + timedelta(hours=1),
            intent_type=IntentType.SWAP,
            executed_price=Decimal("2000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            pnl_usd=None,  # opening / inventory-building
            success=True,
            amount_usd=Decimal("5000"),
            protocol="uniswap_v3",
        ),
        TradeRecord(
            timestamp=_TS0 + timedelta(hours=2),
            intent_type=IntentType.SWAP,
            executed_price=Decimal("2500"),
            fee_usd=Decimal("10"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            pnl_usd=Decimal("1250"),  # gross realized at the 2,500 tick
            success=True,
            amount_usd=Decimal("6250"),
            protocol="uniswap_v3",
        ),
    ]
    nm, initial, final = compute_numeraire_metrics(
        curve,
        numeraire_symbol="WETH",
        trading_days_per_year=365,
        risk_free_rate=Decimal("0"),
    )
    assert nm is not None and initial == Decimal("5") and final == Decimal("4.5")
    return curve, trades, nm


def test_merge_numeraire_canonical_overwrites_primary_fields() -> None:
    from almanak.framework.backtesting.numeraire import merge_numeraire_canonical

    curve, trades, nm = _moving_price_fixture()
    metrics = BacktestMetrics(
        total_pnl_usd=Decimal("1250"),
        net_pnl_usd=Decimal("1250"),
        total_fees_usd=Decimal("10"),
        realized_pnl=Decimal("1250"),
        unrealized_pnl=Decimal("0"),
        failed_trades=0,
    )
    merge_numeraire_canonical(metrics, nm, curve, trades)

    assert metrics.performance_denomination == "WETH"
    assert metrics.numeraire_price_usd_start == Decimal("2000")
    assert metrics.numeraire_price_usd_end == Decimal("2500")
    # Equity-derived: the WETH story (-0.5 WETH == -10%), USD derived at P_end.
    assert metrics.total_pnl_numeraire == Decimal("-0.5")
    assert metrics.total_pnl_usd == Decimal("-1250")
    assert metrics.net_pnl_usd == Decimal("-1250")
    assert metrics.total_return_pct == Decimal("-10")
    assert metrics.max_drawdown_pct == Decimal("0.1")
    # Trade stats convert at the trade tick: +1,240 net at 2,500 -> +0.496 WETH.
    assert metrics.trades_with_realized_pnl == 1
    assert metrics.winning_trades == 1
    assert metrics.win_rate == Decimal("1")
    assert metrics.largest_win_numeraire == Decimal("0.496")
    assert metrics.largest_win_usd == Decimal("1240")
    # Realized uses gross per-trade PnL at the trade tick (1,250 / 2,500).
    assert metrics.realized_pnl_numeraire == Decimal("0.5")
    assert metrics.realized_pnl == Decimal("1250")
    assert metrics.unrealized_pnl_numeraire == Decimal("0")
    # Costs: USD ledger untouched; numeraire column converted at trade tick.
    assert metrics.total_fees_usd == Decimal("10")
    assert metrics.total_fees_numeraire == Decimal("0.004")
    # Attribution buckets numeraire PnLs, expressed at P_end (0.496 x 2500).
    assert metrics.pnl_by_intent_type == {"SWAP": Decimal("1240")}
    # The legacy sub-block is not attached at v3.
    assert metrics.numeraire_metrics is None


def test_merge_trade_price_lookup_uses_most_recent_earlier_point() -> None:
    """Off-grid trade timestamps resolve to the most recent earlier mark."""
    from almanak.framework.backtesting.models import IntentType, TradeRecord
    from almanak.framework.backtesting.numeraire import merge_numeraire_canonical

    curve, _trades, nm = _moving_price_fixture()
    off_grid = TradeRecord(
        timestamp=_TS0 + timedelta(hours=1, minutes=30),  # between the 2,000 and 2,500 marks
        intent_type=IntentType.SWAP,
        executed_price=Decimal("2000"),
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        pnl_usd=Decimal("100"),
        success=True,
        amount_usd=Decimal("100"),
    )
    metrics = BacktestMetrics()
    merge_numeraire_canonical(metrics, nm, curve, [off_grid])
    # 100 USD at the most recent earlier mark (2,000) -> 0.05 WETH.
    assert metrics.realized_pnl_numeraire == Decimal("0.05")


def test_v2_artifact_with_numeraire_metrics_preserves_legacy_block() -> None:
    """A v2 numeraire artifact loads USD-canonical and keeps its sub-block.

    Nothing is re-canonicalized on load: the primary fields keep their stored
    (v2, USD) semantics -- so performance_denomination correctly reads "USD" --
    and the legacy numeraire_metrics sub-block survives a load/save cycle.
    """
    v2_metrics = {
        "schema_version": 2,
        "total_pnl_usd": "1250",
        "total_return_pct": "12.5",
        "numeraire_metrics": {
            "numeraire": "WETH",
            "total_pnl": "-0.5",
            "total_return_pct": "-10",
        },
    }
    result = BacktestResult.from_dict(
        {
            "engine": "pnl",
            "deployment_id": "d",
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-01-01T01:00:00+00:00",
            "metrics": v2_metrics,
        }
    )
    metrics = result.metrics
    # v2 percentages are NOT re-migrated (the x100 rule applies only below v2).
    assert metrics.total_return_pct == Decimal("12.5")
    assert metrics.performance_denomination == "USD"
    assert metrics.total_pnl_numeraire is None
    assert metrics.numeraire_metrics is not None
    assert metrics.numeraire_metrics.total_pnl == Decimal("-0.5")
    # Re-emission keeps the legacy block (now under the v3 schema version).
    payload = metrics.to_dict()
    assert payload["schema_version"] == 3
    assert payload["numeraire_metrics"]["numeraire"] == "WETH"


def test_v2_artifact_with_top_level_numeraire_capitals_keeps_usd_return() -> None:
    """A loaded v2 additive-model artifact must not re-canonicalize on save.

    v2 artifacts carry the top-level ``numeraire`` /
    ``initial_capital_numeraire`` / ``final_capital_numeraire`` fields while
    their stored metrics stay USD-canonical
    (``performance_denomination == "USD"``). The ``total_return_pct`` property
    (and therefore ``to_dict()["total_return_pct"]``) must keep telling the
    stored USD story — gating on the capitals alone would flip it to the
    numeraire return (-10% here) and contradict ``metrics.total_return_pct``.
    """
    payload = {
        "engine": "pnl",
        "deployment_id": "d",
        "start_time": "2024-01-01T00:00:00+00:00",
        "end_time": "2024-01-01T01:00:00+00:00",
        "metrics": {
            "schema_version": 2,
            "total_pnl_usd": "1250",
            "total_return_pct": "12.5",
            "numeraire_metrics": {
                "numeraire": "WETH",
                "total_pnl": "-0.5",
                "total_return_pct": "-10",
            },
        },
        "initial_portfolio_value_usd": "10000",
        "final_capital_usd": "11250",
        "numeraire": "WETH",
        "initial_capital_numeraire": "5",
        "final_capital_numeraire": "4.5",
    }
    result = BacktestResult.from_dict(payload)

    assert result.metrics.performance_denomination == "USD"
    # The property stays USD-canonical, agreeing with the stored v2 metrics.
    assert result.total_return_pct == Decimal("12.5")
    saved = result.to_dict()
    assert Decimal(saved["total_return_pct"]) == Decimal("12.5")
    # The additive v2 fields still round-trip untouched.
    assert saved["numeraire"] == "WETH"
    assert saved["initial_capital_numeraire"] == "5"
    assert saved["final_capital_numeraire"] == "4.5"
    assert saved["metrics"]["numeraire_metrics"]["total_return_pct"] == "-10"

    # Contrast: a fresh v3 token-quoted result (denomination set by the merge)
    # DOES report the numeraire return through the same property.
    v3 = BacktestResult.from_dict(payload)
    v3.metrics.performance_denomination = "WETH"
    assert v3.total_return_pct == Decimal("-10")
