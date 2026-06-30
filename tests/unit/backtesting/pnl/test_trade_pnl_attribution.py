"""Per-trade realized PnL attribution for spot swaps (VIB-5083).

Before this fix ``SimulatedPortfolio._calculate_trade_pnl`` returned
``Decimal("0")`` for every SWAP, so per-trade ``pnl_usd`` was 0, the
metrics layer counted every swap as a loss, ``win_rate`` was 0, and
``largest_win_usd`` could be NEGATIVE. These tests pin the corrected
semantics:

- A SWAP that DISPOSES of a tracked spot token realizes
  ``proceeds - units x average-cost basis``.
- A SWAP that only ACQUIRES inventory realizes nothing yet: ``pnl_usd`` is
  ``None`` (unknown, Empty != Zero), excluded from win/loss stats.
- Rejected fills (``success=False``) are reported as ``failed_trades`` and
  never fold into win/loss stats.
- ``profit_factor`` / ``largest_win_usd`` serialize in normalized form (no
  ``0E+17``, no negative "win").
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.metrics_calculator import calculate_metrics
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedFill,
    SimulatedPortfolio,
)

TS = datetime(2025, 11, 1, tzinfo=UTC)
BASE_USDC_KEY = "base:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
BASE_WETH_KEY = "base:0x4200000000000000000000000000000000000006"


def _market(weth: str) -> MarketState:
    return MarketState(
        timestamp=TS,
        prices={"WETH": Decimal(weth), "USDC": Decimal("1")},
        chain="arbitrum",
    )


def _buy_weth_fill(amount_usd: Decimal, weth_price: Decimal, *, ts: datetime = TS) -> SimulatedFill:
    """USDC -> WETH buy (acquires inventory). Zero costs for clean closed form."""
    units = amount_usd / weth_price
    return SimulatedFill(
        timestamp=ts,
        intent_type=IntentType.SWAP,
        protocol="uniswap_v3",
        tokens=["USDC", "WETH"],
        executed_price=weth_price,
        amount_usd=amount_usd,
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        tokens_in={"WETH": units},
        tokens_out={"USDC": amount_usd},
    )


def _sell_weth_fill(units: Decimal, weth_price: Decimal, *, ts: datetime = TS) -> SimulatedFill:
    """WETH -> USDC sell (disposes inventory). Zero costs for clean closed form."""
    return SimulatedFill(
        timestamp=ts,
        intent_type=IntentType.SWAP,
        protocol="uniswap_v3",
        tokens=["WETH", "USDC"],
        executed_price=weth_price,
        amount_usd=units * weth_price,
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        tokens_in={"USDC": units * weth_price},
        tokens_out={"WETH": units},
    )


@pytest.fixture
def portfolio() -> SimulatedPortfolio:
    return SimulatedPortfolio(initial_capital_usd=Decimal("10000"))


class TestSwapCostBasisAttribution:
    """Realized PnL ties to per-token average cost basis."""

    def test_buy_then_sell_at_profit_records_realized_gain(self, portfolio: SimulatedPortfolio) -> None:
        """Buy 2.5 WETH @ $2,000, sell @ $2,500: closing trade realizes +$1,250."""
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.apply_fill(_sell_weth_fill(Decimal("2.5"), Decimal("2500")), market_state=_market("2500"))

        buy, sell = portfolio.trades
        # The opening buy realized no PnL: unknown, not a fabricated zero.
        assert buy.pnl_usd is None
        assert buy.has_realized_pnl is False
        # The closing sell realized the +$1,250 the round trip locked in.
        assert sell.pnl_usd == Decimal("1250")
        assert sell.has_realized_pnl is True
        # Equity proves the economic reality: 10000 - 5000 (buy) + 6250 (sell).
        assert portfolio.get_total_value_usd(_market("2500")) == Decimal("11250")

    def test_buy_then_sell_at_loss_is_not_mislabeled(self, portfolio: SimulatedPortfolio) -> None:
        """Buy 2.5 WETH @ $2,000, sell @ $1,600: closing trade realizes -$1,000."""
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.apply_fill(_sell_weth_fill(Decimal("2.5"), Decimal("1600")), market_state=_market("1600"))

        buy, sell = portfolio.trades
        assert buy.pnl_usd is None
        # 2.5 x 1600 proceeds (4000) - 2.5 x 2000 basis (5000) = -1000.
        assert sell.pnl_usd == Decimal("-1000")
        assert sell.has_realized_pnl is True

    def test_partial_sell_realizes_against_average_cost(self, portfolio: SimulatedPortfolio) -> None:
        """Two buys at different prices, then a partial sell at the average."""
        # Buy 1 WETH @ 2000, then 1 WETH @ 4000 -> average cost 3000.
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("2000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("4000"), Decimal("4000")), market_state=_market("4000"))
        # Sell 1 WETH @ 3500: proceeds 3500 - 1 x 3000 avg cost = +500.
        assert portfolio.apply_fill(_sell_weth_fill(Decimal("1"), Decimal("3500")), market_state=_market("3500"))

        sell = portfolio.trades[-1]
        assert sell.pnl_usd == Decimal("500")
        # The remaining 1 WETH keeps the $3,000 average cost.
        assert portfolio._cost_basis["WETH"] == Decimal("3000")

    def test_opening_only_swap_has_none_pnl(self, portfolio: SimulatedPortfolio) -> None:
        """A buy with no prior inventory carries pnl_usd None, not 0."""
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        trade = portfolio.trades[-1]
        assert trade.pnl_usd is None
        assert trade.net_pnl_usd is None
        assert trade.has_realized_pnl is False


class TestRejectedTradeMetrics:
    """Rejected fills are reported separately from win/loss stats."""

    def test_rejected_fill_counts_as_failed_not_loss(self, portfolio: SimulatedPortfolio) -> None:
        """Selling WETH the portfolio does not hold is a failed trade."""
        from almanak.framework.backtesting.models import EquityPoint

        # Sell WETH with empty inventory -> rejected (short-from-nothing).
        applied = portfolio.apply_fill(_sell_weth_fill(Decimal("1"), Decimal("2000")), market_state=_market("2000"))
        assert applied is False
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert trade.has_realized_pnl is False

        portfolio.equity_curve = [
            EquityPoint(timestamp=TS, value_usd=Decimal("10000")),
            EquityPoint(timestamp=TS + timedelta(hours=1), value_usd=Decimal("10000")),
        ]
        config = PnLBacktestConfig(
            start_time=TS,
            end_time=TS + timedelta(hours=1),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
        )
        metrics = calculate_metrics(portfolio, portfolio.trades, config)
        # The rejected fill lands in failed_trades and is EXCLUDED from
        # total_trades (successful trades only) -- updated for the VIB-5083
        # CodeRabbit finding that total_trades must not count rejected fills.
        assert metrics.total_trades == 0
        assert metrics.failed_trades == 1
        assert metrics.winning_trades == 0
        assert metrics.losing_trades == 0
        assert metrics.trades_with_realized_pnl == 0


class TestHonestWinLossMetrics:
    """win_rate / largest_win / profit_factor reflect realized economics."""

    def _config(self) -> PnLBacktestConfig:
        return PnLBacktestConfig(
            start_time=TS,
            end_time=TS + timedelta(hours=4),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
        )

    def _run_round_trips(self, portfolio: SimulatedPortfolio) -> None:
        # Profitable round trip: +1250. Assert each setup fill applies so a
        # rejected setup surfaces here, not as a downstream metric mismatch.
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.apply_fill(_sell_weth_fill(Decimal("2.5"), Decimal("2500")), market_state=_market("2500"))
        # Losing round trip: -1000.
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.apply_fill(_sell_weth_fill(Decimal("2.5"), Decimal("1600")), market_state=_market("1600"))

    def test_win_rate_excludes_opening_trades(self, portfolio: SimulatedPortfolio) -> None:
        self._run_round_trips(portfolio)
        from almanak.framework.backtesting.models import EquityPoint

        portfolio.equity_curve = [
            EquityPoint(timestamp=TS, value_usd=Decimal("10000")),
            EquityPoint(timestamp=TS + timedelta(hours=4), value_usd=Decimal("10250")),
        ]
        metrics = calculate_metrics(portfolio, portfolio.trades, self._config())

        # 4 recorded trades, but only the 2 closing sells realized PnL.
        assert metrics.total_trades == 4
        assert metrics.trades_with_realized_pnl == 2
        assert metrics.winning_trades == 1
        assert metrics.losing_trades == 1
        # Win rate is the realized-PnL win fraction, NOT 1/4.
        assert metrics.win_rate == Decimal("0.5")

    def test_largest_win_is_never_negative(self, portfolio: SimulatedPortfolio) -> None:
        self._run_round_trips(portfolio)
        from almanak.framework.backtesting.models import EquityPoint

        portfolio.equity_curve = [
            EquityPoint(timestamp=TS, value_usd=Decimal("10000")),
            EquityPoint(timestamp=TS + timedelta(hours=4), value_usd=Decimal("10250")),
        ]
        metrics = calculate_metrics(portfolio, portfolio.trades, self._config())

        # Largest win is the +1250 realized gain, never a negative "win".
        assert metrics.largest_win_usd == Decimal("1250")
        assert metrics.largest_win_usd > Decimal("0")
        assert metrics.largest_loss_usd == Decimal("-1000")

    def test_profit_factor_and_metrics_serialize_normalized(self, portfolio: SimulatedPortfolio) -> None:
        """profit_factor and largest_win render finite/normalized (no 0E+17)."""
        self._run_round_trips(portfolio)
        from almanak.framework.backtesting.models import EquityPoint

        portfolio.equity_curve = [
            EquityPoint(timestamp=TS, value_usd=Decimal("10000")),
            EquityPoint(timestamp=TS + timedelta(hours=4), value_usd=Decimal("10250")),
        ]
        metrics = calculate_metrics(portfolio, portfolio.trades, self._config())
        d = metrics.to_dict()

        # profit_factor = 1250 / 1000 = 1.25, rendered plainly.
        assert d["profit_factor"] == "1.25"
        assert "E" not in d["largest_win_usd"].upper()
        assert Decimal(d["profit_factor"]) == metrics.profit_factor

    def test_zero_profit_factor_renders_as_zero_not_scientific(self, portfolio: SimulatedPortfolio) -> None:
        """A zero metric never serializes as 0E+17 (the display bug)."""
        # Construct a metrics object whose profit_factor is a zero with an
        # inflated exponent, mirroring how 0 / large-gross-loss arises.
        from almanak.framework.backtesting.models import BacktestMetrics

        metrics = BacktestMetrics(
            profit_factor=Decimal("0E+17"),
            largest_win_usd=Decimal("0E+10"),
        )
        d = metrics.to_dict()
        assert d["profit_factor"] == "0"
        assert d["largest_win_usd"] == "0"


def _trade_record(
    *,
    pnl_usd: Decimal | None,
    success: bool = True,
    protocol: str = "uniswap_v3",
    intent_type: IntentType = IntentType.SWAP,
    tokens: list[str] | None = None,
) -> "object":
    from almanak.framework.backtesting.models import TradeRecord

    return TradeRecord(
        timestamp=TS,
        intent_type=intent_type,
        executed_price=Decimal("2000"),
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        pnl_usd=pnl_usd,
        success=success,
        protocol=protocol,
        tokens=tokens if tokens is not None else ["WETH", "USDC"],
    )


class TestAttributionHandlesUnrealizedPnL:
    """PnL attribution coalesces None (unrealized) to zero at the sum boundary."""

    def test_opening_trade_contributes_zero_to_attribution(self) -> None:
        from almanak.framework.backtesting.pnl.calculators.attribution import (
            attribute_pnl_by_intent_type,
            attribute_pnl_by_protocol,
        )

        trades = [
            _trade_record(pnl_usd=None),  # opening: realized nothing
            _trade_record(pnl_usd=Decimal("1250")),  # closing: realized +1250
        ]
        by_protocol = attribute_pnl_by_protocol(trades, use_net_pnl=True)
        by_intent = attribute_pnl_by_intent_type(trades, use_net_pnl=True)
        # The opening trade adds 0; only the realized +1250 lands in the bucket.
        assert by_protocol == {"uniswap_v3": Decimal("1250")}
        assert by_intent == {"SWAP": Decimal("1250")}

    def test_verify_attribution_totals_with_unrealized_trade(self) -> None:
        from almanak.framework.backtesting.pnl.calculators.attribution import (
            attribute_pnl_by_asset,
            attribute_pnl_by_intent_type,
            attribute_pnl_by_protocol,
            verify_attribution_totals,
        )

        trades = [
            _trade_record(pnl_usd=None),
            _trade_record(pnl_usd=Decimal("1250")),
            _trade_record(pnl_usd=Decimal("-250"), success=False),  # rejected: excluded
        ]
        by_protocol = attribute_pnl_by_protocol(trades)
        by_intent = attribute_pnl_by_intent_type(trades)
        by_asset = attribute_pnl_by_asset(trades)
        # Expected realized total is +1250 (None -> 0, rejected excluded), so
        # all three attributions tie out and the verifier returns True.
        assert verify_attribution_totals(
            trades=trades,
            pnl_by_protocol=by_protocol,
            pnl_by_intent_type=by_intent,
            pnl_by_asset=by_asset,
        )


class TestAttributionResult:
    def test_get_attribution_result_dispatches_protocol_intent_and_asset(self) -> None:
        from almanak.framework.backtesting.pnl.calculators.attribution import AttributionCalculator

        trades = [
            _trade_record(pnl_usd=Decimal("120"), protocol="uniswap_v3", intent_type=IntentType.SWAP),
            _trade_record(
                pnl_usd=Decimal("-20"),
                protocol="aave_v3",
                intent_type=IntentType.BORROW,
                tokens=["USDC"],
            ),
            _trade_record(pnl_usd=Decimal("999"), success=False, protocol="failed"),
        ]
        calculator = AttributionCalculator()

        by_protocol = calculator.get_attribution_result(trades, "protocol")
        by_intent = calculator.get_attribution_result(trades, "intent_type")
        by_asset = calculator.get_attribution_result(trades, "asset")

        assert by_protocol.attribution == {"uniswap_v3": Decimal("120"), "aave_v3": Decimal("-20")}
        assert by_protocol.total_pnl == Decimal("100")
        assert by_protocol.trade_count == 2
        assert by_protocol.unattributed_pnl == Decimal("0")
        assert by_intent.attribution == {"SWAP": Decimal("120"), "BORROW": Decimal("-20")}
        assert by_asset.attribution == {
            "WETH": Decimal("60"),
            "USDC": Decimal("40"),
        }

    def test_get_attribution_result_rejects_unknown_type(self) -> None:
        from almanak.framework.backtesting.pnl.calculators.attribution import AttributionCalculator

        with pytest.raises(ValueError, match="Unknown attribution_type"):
            AttributionCalculator().get_attribution_result([], "venue")


class TestAddressKeyedAssetAttribution:
    def test_address_keys_are_preserved_and_symbols_are_display_labels(self) -> None:
        from almanak.framework.backtesting.pnl.calculators.attribution import (
            attribute_pnl_by_asset,
            attribute_pnl_by_asset_display_labels,
        )

        trades = [
            _trade_record(
                pnl_usd=Decimal("100"),
                tokens=[BASE_USDC_KEY, BASE_WETH_KEY],
            )
        ]

        by_asset = attribute_pnl_by_asset(trades)
        display_labels = attribute_pnl_by_asset_display_labels(trades)

        assert by_asset == {
            BASE_USDC_KEY: Decimal("50"),
            BASE_WETH_KEY: Decimal("50"),
        }
        assert display_labels == {
            BASE_USDC_KEY: "USDC",
            BASE_WETH_KEY: "WETH",
        }

    def test_metrics_serialize_address_attribution_with_display_labels(self) -> None:
        from almanak.framework.backtesting.models import BacktestMetrics, BacktestResult

        metrics = BacktestMetrics(
            pnl_by_asset={BASE_USDC_KEY: Decimal("12.34")},
            max_net_delta={BASE_WETH_KEY: Decimal("0.5")},
        )

        serialized = metrics.to_dict()
        restored = BacktestResult._parse_metrics(serialized)

        assert serialized["pnl_by_asset"] == {BASE_USDC_KEY: "12.34"}
        assert serialized["pnl_by_asset_display_labels"] == {BASE_USDC_KEY: "USDC"}
        assert serialized["max_net_delta"] == {BASE_WETH_KEY: "0.5"}
        assert serialized["max_net_delta_display_labels"] == {BASE_WETH_KEY: "WETH"}
        assert restored.pnl_by_asset == {BASE_USDC_KEY: Decimal("12.34")}
        assert restored.pnl_by_asset_display_labels == {BASE_USDC_KEY: "USDC"}
        assert restored.max_net_delta == {BASE_WETH_KEY: Decimal("0.5")}
        assert restored.max_net_delta_display_labels == {BASE_WETH_KEY: "WETH"}

    def test_legacy_symbols_are_uppercased_for_keys_and_labels(self) -> None:
        from almanak.framework.backtesting.pnl.calculators.attribution import (
            attribute_pnl_by_asset,
            attribute_pnl_by_asset_display_labels,
        )

        trades = [
            _trade_record(
                pnl_usd=Decimal("100"),
                tokens=["weth", "usdc"],
            )
        ]

        assert attribute_pnl_by_asset(trades) == {
            "WETH": Decimal("50"),
            "USDC": Decimal("50"),
        }
        assert attribute_pnl_by_asset_display_labels(trades) == {
            "WETH": "WETH",
            "USDC": "USDC",
        }


class TestGeminiReviewRegressions:
    """Pins for the two HIGH findings on PR #2805 (Gemini review)."""

    def test_special_decimals_serialize_without_crashing(self) -> None:
        """_decimal_str must pass NaN/Infinity through, not raise InvalidOperation.

        Ratio metrics (Sharpe, Sortino, information ratio) legitimately reach
        Infinity at zero volatility; normalize() raises on special values.
        """
        from almanak.framework.backtesting.models import _decimal_str

        assert _decimal_str(Decimal("Infinity")) == "Infinity"
        assert _decimal_str(Decimal("-Infinity")) == "-Infinity"
        assert _decimal_str(Decimal("NaN")) == "NaN"
        # Sanity: finite values still normalize as before.
        assert _decimal_str(Decimal("0E+17")) == "0"
        assert _decimal_str(Decimal("1000").normalize()) == "1000"

    def test_multi_token_out_swap_does_not_double_count_proceeds(self) -> None:
        """A swap disposing of two tracked tokens splits proceeds pro-rata.

        Pre-fix, the full in_value was added once per disposed leg, inflating
        realized PnL. Build WETH + WBTC inventory at cost, then dispose both
        for USDC in one fill; realized must equal (proceeds - total cost),
        not (proceeds x 2 - total cost).
        """
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))
        market = MarketState(
            timestamp=TS,
            prices={"WETH": Decimal("2000"), "WBTC": Decimal("40000"), "USDC": Decimal("1")},
            chain="arbitrum",
            block_number=1,
            gas_price_gwei=Decimal("30"),
        )
        # Seed inventory at known cost: 1 WETH @ $2000, 0.1 WBTC @ $40000.
        portfolio.tokens["WETH"] = Decimal("1")
        portfolio.tokens["WBTC"] = Decimal("0.1")
        portfolio._cost_basis["WETH"] = Decimal("2000")
        portfolio._cost_basis["WBTC"] = Decimal("40000")

        # Dispose both at unchanged prices for $6,000 USDC: proceeds == cost,
        # so realized PnL must be exactly 0 (not +6000 from double counting).
        fill = SimulatedFill(
            intent_type=IntentType.SWAP,
            timestamp=TS,
            protocol="uniswap_v3",
            tokens=["WETH", "WBTC", "USDC"],
            amount_usd=Decimal("6000"),
            tokens_out={"WETH": Decimal("1"), "WBTC": Decimal("0.1")},
            tokens_in={"USDC": Decimal("6000")},
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
        )
        assert portfolio.apply_fill(fill, market)

        assert Decimal(fill.metadata["realized_pnl_usd"]) == Decimal("0")


class TestCodeRabbitReviewRegressions:
    """Pins for the CodeRabbit findings on PR #2805 (VIB-5083 review round)."""

    def test_decimal_str_preserves_high_precision_round_trip(self) -> None:
        """A 30-significant-digit Decimal round-trips without precision loss.

        ``normalize()`` re-rounds the coefficient to the active context
        precision (28 digits by default), silently dropping digits. Fixed-point
        ``format(value, "f")`` keeps every stored digit (comment_id 3410921730).
        """
        from almanak.framework.backtesting.models import _decimal_str

        value = Decimal("1.23456789012345678901234567891")  # 30 significant digits
        rendered = _decimal_str(value)
        assert Decimal(rendered) == value
        # Fixed-point, never scientific, in both directions.
        assert _decimal_str(Decimal("1E+3")) == "1000"
        assert _decimal_str(Decimal("1E-7")) == "0.0000001"
        # Trailing fractional zeros are trimmed; integers are untouched.
        assert _decimal_str(Decimal("1.2300")) == "1.23"
        assert _decimal_str(Decimal("1.000")) == "1"

    def test_largest_win_zero_on_all_loss_run(self) -> None:
        """An all-loss run reports largest_win == 0, not a negative "win".

        Pre-fix max/min ran over the undirected realized list, so the least
        negative loss surfaced as the "largest win" (comment_id 3410921761).
        """
        from almanak.framework.backtesting.pnl.metrics_calculator import (
            _compute_trade_statistics,
        )

        losers = [_trade_record(pnl_usd=Decimal("-200")), _trade_record(pnl_usd=Decimal("-50"))]
        stats = _compute_trade_statistics(losers)
        assert stats.largest_win == Decimal("0")
        assert stats.largest_loss == Decimal("-200")

    def test_largest_loss_zero_on_all_win_run(self) -> None:
        """An all-win run reports largest_loss == 0, not a positive "loss"."""
        from almanak.framework.backtesting.pnl.metrics_calculator import (
            _compute_trade_statistics,
        )

        winners = [_trade_record(pnl_usd=Decimal("200")), _trade_record(pnl_usd=Decimal("50"))]
        stats = _compute_trade_statistics(winners)
        assert stats.largest_loss == Decimal("0")
        assert stats.largest_win == Decimal("200")

    def test_metrics_rehydrate_derives_missing_realized_denominator(self) -> None:
        """An old artifact lacking trades_with_realized_pnl rehydrates consistently.

        The denominator defaults to winning + losing rather than 0 when the
        field is absent (comment_id 3410921740).
        """
        from almanak.framework.backtesting.models import BacktestResult

        legacy = {"winning_trades": 3, "losing_trades": 2}  # no trades_with_realized_pnl
        metrics = BacktestResult._parse_metrics(legacy)
        assert metrics.trades_with_realized_pnl == 5

    def test_portfolio_round_trip_with_opening_trade_null_pnl(self) -> None:
        """A portfolio with a None-pnl opening trade survives to_dict/from_dict.

        The serializer emits ``null`` for pnl_usd; from_dict must rehydrate it
        as None instead of crashing on ``Decimal(None)`` (comment_id 3410921736).
        """
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        # Opening buy: realizes nothing (pnl_usd None).
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.trades[-1].pnl_usd is None

        restored = SimulatedPortfolio.from_dict(portfolio.to_dict())
        assert restored.trades[-1].pnl_usd is None

    def test_cost_basis_survives_serialization_and_realizes_later(self) -> None:
        """A resumed portfolio keeps its average cost so later sells realize PnL.

        _cost_basis was omitted from to_dict/from_dict, so a resumed portfolio
        forgot its average costs and a later disposing sell realized nothing
        (comment_id 3410921765).
        """
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        # Build basis: buy 2.5 WETH @ $2,000.
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio._cost_basis["WETH"] == Decimal("2000")

        restored = SimulatedPortfolio.from_dict(portfolio.to_dict())
        assert restored._cost_basis["WETH"] == Decimal("2000")

        # A subsequent sell on the RESTORED portfolio realizes against the
        # restored average cost (2.5 x 2500 - 2.5 x 2000 = +1250).
        assert restored.apply_fill(_sell_weth_fill(Decimal("2.5"), Decimal("2500")), market_state=_market("2500"))
        assert restored.trades[-1].pnl_usd == Decimal("1250")

    def test_swap_realized_pnl_aggregates_into_portfolio_realized_pnl(self) -> None:
        """A profitable spot round trip lifts BacktestMetrics.realized_pnl.

        SWAP disposals realize PnL via metadata but never reached _realized_pnl
        (only position closes did), understating the aggregate (comment_id
        3410921770).
        """
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.apply_fill(_sell_weth_fill(Decimal("2.5"), Decimal("2500")), market_state=_market("2500"))

        closing = portfolio.trades[-1]
        assert closing.pnl_usd == Decimal("1250")
        # The portfolio-level realized total equals the closing trade's gain.
        assert portfolio._realized_pnl == Decimal("1250")

    def test_unpriced_disposal_inflow_leaves_pnl_none(self) -> None:
        """A disposal whose proceeds cannot be priced realizes None, not a loss.

        When the inflow leg has tokens but no price, in_value is 0 and the old
        code booked ``proceeds - cost`` as a fabricated loss. The guard leaves
        realized_pnl_usd unset so pnl_usd stays None (comment_id 3410921773).
        """
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        # Seed 1 WETH at a known $2,000 cost.
        portfolio.tokens["WETH"] = Decimal("1")
        portfolio._cost_basis["WETH"] = Decimal("2000")
        # Dispose WETH for an UNPRICED token (MYSTERY absent from market prices).
        market = MarketState(timestamp=TS, prices={"WETH": Decimal("2000")}, chain="arbitrum")
        fill = SimulatedFill(
            intent_type=IntentType.SWAP,
            timestamp=TS,
            protocol="uniswap_v3",
            tokens=["WETH", "MYSTERY"],
            amount_usd=Decimal("2000"),
            tokens_out={"WETH": Decimal("1")},
            tokens_in={"MYSTERY": Decimal("123")},
            executed_price=Decimal("2000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
        )
        assert portfolio.apply_fill(fill, market)
        # No fabricated loss: proceeds were unpriceable, so PnL is unknown.
        assert "realized_pnl_usd" not in fill.metadata
        assert portfolio.trades[-1].pnl_usd is None

    def test_swap_trade_record_carries_actual_amounts(self) -> None:
        """A SWAP TradeRecord surfaces actual_amount_in / actual_amount_out.

        to_trade_record left them None though tokens_out (paid) / tokens_in
        (received) are known; downstream _build_swap_amounts needs them
        (comment_id 28b40a63).
        """
        fill = SimulatedFill(
            intent_type=IntentType.SWAP,
            timestamp=TS,
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            amount_usd=Decimal("2000"),
            tokens_out={"USDC": Decimal("2000")},
            tokens_in={"WETH": Decimal("1")},
            executed_price=Decimal("2000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
        )
        record = fill.to_trade_record(pnl_usd=None)
        assert record.actual_amount_in == Decimal("2000")  # paid (tokens_out)
        assert record.actual_amount_out == Decimal("1")  # received (tokens_in)

    def test_mixed_priced_unpriced_disposal_does_not_overallocate_proceeds(self) -> None:
        """A disposed set with one priced and one unpriced leg uses one mode.

        Pre-fix the priced leg took a pro-rata share of the FULL in_value while
        the unpriced leg also drew an even-split share, so total allocated
        proceeds exceeded the actual proceeds (comment_id 3410947624). With a
        single even-split mode the realized PnL stays bounded by in_value.
        """
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))
        # WETH is priced; RARE is a tracked spot token with no market price.
        market = MarketState(timestamp=TS, prices={"WETH": Decimal("2000"), "USDC": Decimal("1")}, chain="arbitrum")
        portfolio.tokens["WETH"] = Decimal("1")
        portfolio.tokens["RARE"] = Decimal("10")
        portfolio._cost_basis["WETH"] = Decimal("2000")
        portfolio._cost_basis["RARE"] = Decimal("100")

        # Dispose both for $3,000 USDC proceeds. Total cost = 2000 + 1000 = 3000,
        # so realized PnL must be exactly 0 -- not inflated by double-allocation.
        fill = SimulatedFill(
            intent_type=IntentType.SWAP,
            timestamp=TS,
            protocol="uniswap_v3",
            tokens=["WETH", "RARE", "USDC"],
            amount_usd=Decimal("3000"),
            tokens_out={"WETH": Decimal("1"), "RARE": Decimal("10")},
            tokens_in={"USDC": Decimal("3000")},
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
        )
        assert portfolio.apply_fill(fill, market)
        # proceeds (3000) split evenly is 1500 each; 1500 - 2000 (WETH cost) +
        # 1500 - 1000 (RARE cost) = -500 + 500 = 0. Never > in_value.
        realized = Decimal(fill.metadata["realized_pnl_usd"])
        assert realized == Decimal("0")


class TestCodeRabbitRound3Regressions:
    """Pins for the third CodeRabbit round on PR #2805 (coordinator review pass)."""

    def test_total_trades_excludes_rejected_fills(self, portfolio: SimulatedPortfolio) -> None:
        """total_trades counts successful trades only; rejected fills -> failed_trades."""
        from almanak.framework.backtesting.models import EquityPoint

        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        # Sell more WETH than held -> rejected, recorded as a failed trade.
        assert not portfolio.apply_fill(_sell_weth_fill(Decimal("99"), Decimal("2000")), market_state=_market("2000"))
        # get_metrics short-circuits without an equity curve; seed a flat one.
        portfolio.equity_curve = [
            EquityPoint(timestamp=TS, value_usd=Decimal("10000")),
            EquityPoint(timestamp=TS + timedelta(hours=1), value_usd=Decimal("10000")),
        ]

        metrics = portfolio.get_metrics()

        assert metrics.failed_trades == 1
        assert metrics.total_trades == 1  # the buy only; the rejected sell excluded

    def test_realized_pnl_survives_serialization(self, portfolio: SimulatedPortfolio) -> None:
        """A resumed portfolio reports its accumulated realized_pnl, not 0."""
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.apply_fill(_sell_weth_fill(Decimal("2.5"), Decimal("2500")), market_state=_market("2500"))
        realized_before = portfolio._realized_pnl
        assert realized_before > Decimal("0")  # +1250 round trip

        resumed = SimulatedPortfolio.from_dict(portfolio.to_dict())

        assert resumed._realized_pnl == realized_before

    def test_realized_pnl_falls_back_for_legacy_artifacts(self, portfolio: SimulatedPortfolio) -> None:
        """Artifacts predating the field derive realized_pnl from successful trades."""
        assert portfolio.apply_fill(_buy_weth_fill(Decimal("5000"), Decimal("2000")), market_state=_market("2000"))
        assert portfolio.apply_fill(_sell_weth_fill(Decimal("2.5"), Decimal("2500")), market_state=_market("2500"))
        data = portfolio.to_dict()
        data.pop("realized_pnl")  # simulate an older serialized artifact

        resumed = SimulatedPortfolio.from_dict(data)

        assert resumed._realized_pnl == portfolio._realized_pnl

    def test_unpriced_paid_leg_does_not_seed_zero_cost_basis(self) -> None:
        """An acquisition whose paid leg can't be priced leaves the basis unset.

        Otherwise a later sale realizes the full proceeds as a fabricated gain.
        """
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        portfolio.tokens["WBTC"] = Decimal("1")  # held, but no price for WBTC
        # Acquire WETH by paying WBTC; market prices WETH but NOT WBTC -> out_value 0.
        market = MarketState(
            timestamp=TS,
            prices={"WETH": Decimal("2000")},
            chain="arbitrum",
            block_number=1,
            gas_price_gwei=Decimal("30"),
        )
        acquire = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["WBTC", "WETH"],
            executed_price=Decimal("20"),
            amount_usd=Decimal("0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={"WETH": Decimal("1")},
            tokens_out={"WBTC": Decimal("1")},
        )
        assert portfolio.apply_fill(acquire, market)

        assert "WETH" not in portfolio._cost_basis  # no fabricated zero basis

    def test_multi_leg_swap_record_leaves_actual_amounts_unset(self) -> None:
        """A SWAP disposing of two tokens must not persist an arbitrary single amount."""
        fill = SimulatedFill(
            timestamp=TS,
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["WETH", "WBTC", "USDC"],
            executed_price=Decimal("1"),
            amount_usd=Decimal("6000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={"USDC": Decimal("6000")},
            tokens_out={"WETH": Decimal("1"), "WBTC": Decimal("0.1")},
        )

        record = fill.to_trade_record(pnl_usd=None)

        assert record.actual_amount_in is None  # two disposed legs -> ambiguous
        assert record.actual_amount_out == Decimal("6000")  # single received leg
