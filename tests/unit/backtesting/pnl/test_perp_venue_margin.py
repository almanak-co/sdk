"""Per-venue margin floors actually govern perp opens.

The validator has always carried a per-protocol margin table, but the open
gate validated every venue at the generic 10% default — the table was never
consulted. Observed on staging: a legitimately-levered 10x GMX open was
rejected on 100% of attempts (GMX V2's on-chain floor is 0.5%; the table's
1% is deliberately conservative), with boundary-dust messages like
"10.00% < 10.00% required, need $0.00 more collateral".

Pinned contracts:
- The open gate resolves the initial-margin floor from the intent's protocol.
- Venue values match verified sources (GMX V2 DataStore, Hyperliquid meta
  API, dYdX v4 params) as of 2026-07.
- Unknown protocols keep the conservative default.
- Rejection messages carry true (unrounded) deltas.
- Position liquidation updates use the position's protocol, not a flat
  config override.
"""

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.backtesting.adapters.perp_adapter import (
    PerpBacktestAdapter,
    PerpBacktestConfig,
)
from almanak.framework.backtesting.pnl.calculators.margin import MarginValidator
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.intents.vocabulary import PerpOpenIntent

TS = datetime(2026, 4, 21, 2, 0, tzinfo=UTC)


def _market() -> MarketState:
    return MarketState(timestamp=TS, prices={"WETH": Decimal("2000"), "USDC": Decimal("1")}, chain="arbitrum")


def _intent(size_usd: str, collateral: str, protocol: str = "gmx_v2") -> PerpOpenIntent:
    return PerpOpenIntent(
        market="ETH/USD",
        collateral_token="USDC",
        collateral_amount=Decimal(collateral),
        size_usd=Decimal(size_usd),
        leverage=Decimal(size_usd) / Decimal(collateral),
        protocol=protocol,
    )


def _adapter() -> PerpBacktestAdapter:
    return PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp"))


class TestVenueFloorGovernsOpens:
    def test_exact_10x_gmx_open_passes(self) -> None:
        """The staging shape: $2 collateral / $20 size on GMX. The venue
        floor (1%) allows 100x; 10x must sail through."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("4"))
        fill = _adapter().execute_intent(_intent("20", "2"), portfolio, _market())
        assert fill is None  # validation passed; default execution proceeds

    def test_50x_gmx_open_passes_within_venue_floor(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("4"))
        fill = _adapter().execute_intent(_intent("100", "2"), portfolio, _market())
        assert fill is None

    def test_over_venue_floor_rejects_on_gmx(self) -> None:
        """150x exceeds even the conservative 1% floor (100x). The intent
        layer already refuses to CONSTRUCT >100x gmx_v2 intents (defense in
        depth), so exercise the adapter gate with a duck intent — the gate
        matters when fill-time collateral repricing drifts an at-cap open
        over the floor."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("4"))
        # model_construct skips the intent-layer leverage validation, the
        # same state a fill-time collateral repricing can produce.
        drifted = PerpOpenIntent.model_construct(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("2"),
            size_usd=Decimal("300"),
            leverage=Decimal("150"),
            is_long=True,
            protocol="gmx_v2",
        )
        fill = _adapter()._execute_perp_open(drifted, portfolio, _market())
        assert fill is not None and fill.success is False
        assert "Insufficient margin" in fill.metadata["failure_reason"]

    def test_hyperliquid_floor_is_4pct(self) -> None:
        """Hyperliquid's real ETH cap is 25x: 20x passes, 26x rejects."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("40"))
        ok = _adapter().execute_intent(_intent("400", "20", protocol="hyperliquid"), portfolio, _market())
        assert ok is None
        too_high = _adapter().execute_intent(_intent("520", "20", protocol="hyperliquid"), portfolio, _market())
        assert too_high is not None and too_high.success is False

    def test_unknown_protocol_keeps_conservative_default(self) -> None:
        """No table entry -> the generic 10% default still governs."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("40"))
        fill = _adapter().execute_intent(_intent("400", "20", protocol="unknown_venue"), portfolio, _market())
        assert fill is not None and fill.success is False


class TestMessageHonesty:
    def test_boundary_dust_shortfall_is_never_zero(self) -> None:
        """A hair-below-the-floor miss must show its true delta, not
        'X% < X% required, need $0.00 more'."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("20.00036443383127935262498610"),
            collateral=Decimal("1.999998357733022114354393592"),
            margin_ratio=Decimal("0.1"),
        )
        assert result.is_valid is False
        assert "$0.00 more" not in result.message.replace("$0.000000", "SENTINEL")
        # The displayed percentages must differ (unrounded), and the
        # shortfall must be a real positive figure.
        assert "9.9998" in result.message
        assert result.shortfall > 0

    def test_capital_near_miss_shows_true_delta(self) -> None:
        validator = MarginValidator()
        can_open, reason = validator.can_open_position(
            position_size=Decimal("20"),
            collateral=Decimal("2.000549101567689654278011484"),
            available_capital=Decimal("2.0004"),
            margin_ratio=Decimal("0.01"),
        )
        assert can_open is False
        assert "short $" in reason
        assert "need $2.00 but only $2.00 available" not in reason

    def test_utilization_message_explains_the_cap(self) -> None:
        validator = MarginValidator()
        can_open, reason = validator.can_open_position(
            position_size=Decimal("20"),
            collateral=Decimal("2"),
            available_capital=Decimal("2"),
            margin_ratio=Decimal("0.1"),
        )
        assert can_open is False
        assert "margin capital" in reason
        assert "cash buffer" in reason


class TestLiquidationUsesPositionProtocol:
    def test_position_update_resolves_maintenance_from_protocol(self) -> None:
        """Exercised through the adapter's own update path (the changed call):
        two identical positions on different venues get venue-resolved
        liquidation prices — 10x long from $2,000 entry gives
        entry*(1 - 1/10 + maintenance): $1,820 at GMX's 1%, $1,840 at
        Hyperliquid's 2%. Restoring the flat config override breaks this."""
        from tests.unit.backtesting.adapters.test_perp_adapter import (
            MockMarketState,
            create_perp_long_position,
        )

        adapter = _adapter()
        gmx = create_perp_long_position(
            collateral_usd=Decimal("1000"), leverage=Decimal("10"), entry_price=Decimal("2000"), entry_time=TS
        )
        gmx.protocol = "gmx_v2"
        hl = create_perp_long_position(
            collateral_usd=Decimal("1000"), leverage=Decimal("10"), entry_price=Decimal("2000"), entry_time=TS
        )
        hl.protocol = "hyperliquid"
        market = MockMarketState(prices={"ETH": Decimal("2000")})

        adapter.update_position(gmx, market, elapsed_seconds=3600)
        adapter.update_position(hl, market, elapsed_seconds=3600)

        assert gmx.liquidation_price == Decimal("1820.00")
        assert hl.liquidation_price == Decimal("1840.00")

    def test_liquidation_check_before_first_update_is_protocol_aware(self) -> None:
        """A position checked before any update must resolve its liquidation
        price from its protocol, not the flat config maintenance ratio: a
        Hyperliquid 10x long from $2,000 liquidates at $1,840, so $1,860
        must NOT liquidate (the flat 5% config put the trigger at $1,900)."""
        from tests.unit.backtesting.adapters.test_perp_adapter import create_perp_long_position

        adapter = _adapter()
        hl = create_perp_long_position(
            collateral_usd=Decimal("1000"), leverage=Decimal("10"), entry_price=Decimal("2000"), entry_time=TS
        )
        hl.protocol = "hyperliquid"
        hl.liquidation_price = None

        event = adapter.check_and_simulate_liquidation(hl, current_price=Decimal("1860"), timestamp=TS)
        assert event is None and hl.is_liquidated is False
        assert hl.liquidation_price == Decimal("1840.00")

        event = adapter.check_and_simulate_liquidation(hl, current_price=Decimal("1830"), timestamp=TS)
        assert event is not None and hl.is_liquidated is True
