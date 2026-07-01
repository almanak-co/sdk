"""Tests for real PerpOpenIntent / PerpCloseIntent support in the PnL backtester.

VIB-5079: real perp intents executed through ``PnLBacktester._execute_intent``'s
generic lane were inert — amount extraction found no matching attribute
(``size_usd`` / ``collateral_amount`` were not scanned), token extraction had no
mapping for ``market`` ("ETH/USD"), and PERP_CLOSE could never match a simulated
position because venue position ids (0x tradeHash) never equal simulated ids
("PERP_LONG_gmx_v2_ETH_...").

Design contract encoded here (money-math semantics):

1. ``fill.amount_usd`` for perps is the **notional** (``size_usd``) — it is the
   fee / slippage / MEV base, matching how venues charge fees on position size.
2. The simulated position's ``collateral_usd`` comes from
   ``collateral_amount * price(collateral_token)``; ``"all"`` or unpriceable
   collateral falls back to ``size_usd / leverage``. Effective leverage is
   derived as ``size_usd / collateral_usd`` so ``notional_usd == size_usd``
   exactly.
3. The position's priced token is the base symbol parsed from ``market``
   ("ETH/USD" -> "ETH"), consistent with ``PerpBacktestAdapter``. Address-style
   markets keep the existing UNKNOWN sentinel (entry-price-flat) instead of
   mispricing the position off the collateral token.
4. PERP_CLOSE resolves its target against the portfolio: exact simulated-id
   match first, otherwise (base token, side, protocol) with FIFO tie-break.
   Venue tradeHashes never block matching. Full closes (``size_usd=None``)
   take their fee notional from the matched position.
5. Perp slippage is adverse per side: open long / close short executes above
   market, open short / close long below.

Conservation of collateral in ``apply_fill`` is covered separately
(PR #2744 / test_perp_conservation.py); these tests assert the values
*reaching* that machinery, not cash movement.
"""
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (


    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.backtesting.pnl.position_models import PositionType
from almanak.framework.intents.perp_intents import PerpCloseIntent, PerpOpenIntent

ETH_PRICE = Decimal("3000")
T0 = datetime(2024, 1, 1, tzinfo=UTC)


class MockDataProvider:
    """Minimal data provider; _execute_intent never touches it."""

    provider_name = "mock"

    async def iterate(self, config: Any):
        if False:
            yield


def make_backtester(fee_pct: Decimal = Decimal("0"), slippage_pct: Decimal = Decimal("0")) -> PnLBacktester:
    return PnLBacktester(
        MockDataProvider(),
        {"default": DefaultFeeModel(fee_pct=fee_pct)},
        {"default": DefaultSlippageModel(slippage_pct=slippage_pct)},
    )


def make_market_state(timestamp: datetime = T0) -> MarketState:
    return MarketState(
        timestamp=timestamp,
        prices={"ETH": ETH_PRICE, "WETH": ETH_PRICE, "USDC": Decimal("1")},
    )


def make_config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=T0,
        end_time=T0 + timedelta(days=1),
        token_funding=_pnl_token_funding(Decimal("100000")),
        include_gas_costs=False,
    )


def make_open_intent(**overrides: Any) -> PerpOpenIntent:
    params: dict[str, Any] = {
        "market": "ETH/USD",
        "collateral_token": "USDC",
        "collateral_amount": Decimal("1000"),
        "size_usd": Decimal("5000"),
        "is_long": True,
        "leverage": Decimal("5"),
        "protocol": "gmx_v2",
    }
    params.update(overrides)
    return PerpOpenIntent(**params)


def make_close_intent(**overrides: Any) -> PerpCloseIntent:
    params: dict[str, Any] = {
        "market": "ETH/USD",
        "collateral_token": "USDC",
        "is_long": True,
        "protocol": "gmx_v2",
    }
    params.update(overrides)
    return PerpCloseIntent(**params)


async def execute(
    backtester: PnLBacktester,
    intent: Any,
    portfolio: SimulatedPortfolio,
    market_state: MarketState | None = None,
    timestamp: datetime = T0,
):
    return await backtester._execute_intent(
        intent=intent,
        portfolio=portfolio,
        market_state=market_state or make_market_state(timestamp),
        timestamp=timestamp,
        config=make_config(),
    )


# ---------------------------------------------------------------------------
# Amount extraction: notional comes from size_usd
# ---------------------------------------------------------------------------


class TestPerpAmountExtraction:
    def test_perp_open_amount_is_size_usd(self):
        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_amount_usd

        amount = get_intent_amount_usd(make_open_intent(), make_market_state())
        assert amount == Decimal("5000")

    def test_perp_close_amount_is_size_usd_when_partial(self):
        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_amount_usd

        amount = get_intent_amount_usd(make_close_intent(size_usd=Decimal("2500")), make_market_state())
        assert amount == Decimal("2500")

    def test_perp_open_amount_strict_mode_does_not_raise(self):
        """size_usd is a direct USD field: strict reproducibility must accept it."""
        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_amount_usd

        amount = get_intent_amount_usd(make_open_intent(), make_market_state(), strict_reproducibility=True)
        assert amount == Decimal("5000")

    def test_size_usd_wins_over_collateral_usd(self):
        """For duck-typed perp objects carrying both, the notional (size_usd)
        is the trade amount - collateral must not shadow it."""
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_amount_usd

        intent = SimpleNamespace(size_usd=Decimal("5000"), collateral_usd=Decimal("1000"))
        assert get_intent_amount_usd(intent, make_market_state()) == Decimal("5000")


# ---------------------------------------------------------------------------
# Token extraction: base symbol from market, collateral after
# ---------------------------------------------------------------------------


class TestPerpTokenExtraction:
    def test_open_intent_base_token_first(self):
        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_tokens

        tokens = get_intent_tokens(make_open_intent())
        assert tokens[0] == "ETH"
        assert "USDC" in tokens

    def test_close_intent_base_token_first(self):
        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_tokens

        tokens = get_intent_tokens(make_close_intent())
        assert tokens[0] == "ETH"

    def test_hyphen_separated_market(self):
        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_tokens

        tokens = get_intent_tokens(make_open_intent(market="ETH-USD"))
        assert tokens[0] == "ETH"

    def test_bare_symbol_market(self):
        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_tokens

        tokens = get_intent_tokens(make_open_intent(market="ETH"))
        assert tokens[0] == "ETH"

    def test_address_market_keeps_unknown_sentinel(self):
        """An address market cannot be priced; the collateral token must NOT
        become the priced token (a USDC-priced perp would show zero PnL while
        looking healthy)."""
        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_tokens

        tokens = get_intent_tokens(make_open_intent(market="0x47c031236e19d024b42f8AE6780E44A573170703"))
        assert tokens == ["UNKNOWN"]


# ---------------------------------------------------------------------------
# Executed price: adverse slippage per side
# ---------------------------------------------------------------------------


class TestPerpSlippageDirection:
    SLIP = Decimal("0.005")

    def _price(self, intent: Any, intent_type: IntentType) -> Decimal:
        from almanak.framework.backtesting.pnl.intent_extraction import get_executed_price

        return get_executed_price(intent, make_market_state(), self.SLIP, intent_type)

    def test_open_long_buys_above_market(self):
        price = self._price(make_open_intent(is_long=True), IntentType.PERP_OPEN)
        assert price == ETH_PRICE * (Decimal("1") + self.SLIP)

    def test_open_short_sells_below_market(self):
        price = self._price(make_open_intent(is_long=False), IntentType.PERP_OPEN)
        assert price == ETH_PRICE * (Decimal("1") - self.SLIP)

    def test_close_long_sells_below_market(self):
        price = self._price(make_close_intent(is_long=True), IntentType.PERP_CLOSE)
        assert price == ETH_PRICE * (Decimal("1") - self.SLIP)

    def test_close_short_buys_above_market(self):
        price = self._price(make_close_intent(is_long=False), IntentType.PERP_CLOSE)
        assert price == ETH_PRICE * (Decimal("1") + self.SLIP)


# ---------------------------------------------------------------------------
# PERP_OPEN through _execute_intent: position values
# ---------------------------------------------------------------------------


class TestPerpOpenExecution:
    @pytest.mark.asyncio
    async def test_open_creates_position_with_collateral_and_notional(self):
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        trade = await execute(backtester, make_open_intent(), portfolio)

        assert trade.success
        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        assert position.position_type == PositionType.PERP_LONG
        # 1000 USDC * $1 collateral; notional == size_usd exactly
        assert position.collateral_usd == Decimal("1000")
        assert position.notional_usd == Decimal("5000")
        assert position.leverage == Decimal("5")
        assert position.tokens[0] == "ETH"
        assert position.entry_price == ETH_PRICE  # zero slippage fixture

    @pytest.mark.asyncio
    async def test_open_short_creates_short_position(self):
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(is_long=False), portfolio)

        assert portfolio.positions[0].position_type == PositionType.PERP_SHORT

    @pytest.mark.asyncio
    async def test_open_collateral_priced_in_collateral_token(self):
        """2 WETH collateral at $3000 -> $6000 collateral, leverage derived 2x."""
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))
        intent = make_open_intent(
            collateral_token="WETH",
            collateral_amount=Decimal("2"),
            size_usd=Decimal("12000"),
            leverage=Decimal("2"),
        )

        await execute(backtester, intent, portfolio)

        position = portfolio.positions[0]
        assert position.collateral_usd == Decimal("6000")
        assert position.notional_usd == Decimal("12000")
        assert position.leverage == Decimal("2")

    @pytest.mark.asyncio
    async def test_open_collateral_all_falls_back_to_size_over_leverage(self):
        """Chained 'all' collateral cannot be resolved in the generic lane;
        size_usd / leverage is the deterministic fallback."""
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))
        intent = make_open_intent(collateral_amount="all")

        await execute(backtester, intent, portfolio)

        position = portfolio.positions[0]
        assert position.collateral_usd == Decimal("1000")  # 5000 / 5
        assert position.notional_usd == Decimal("5000")

    @pytest.mark.asyncio
    async def test_open_fee_charged_on_notional(self):
        """Fees are charged on size (venue semantics), not on collateral."""
        backtester = make_backtester(fee_pct=Decimal("0.001"))
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        trade = await execute(backtester, make_open_intent(), portfolio)

        assert trade.amount_usd == Decimal("5000")
        assert trade.fee_usd == Decimal("5")  # 0.1% of 5000, not of 1000


# ---------------------------------------------------------------------------
# PERP_CLOSE through _execute_intent: position matching
# ---------------------------------------------------------------------------


class TestPerpCloseExecution:
    @pytest.mark.asyncio
    async def test_close_matches_open_position_by_market_and_side(self):
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(), portfolio)
        opened_id = portfolio.positions[0].position_id

        trade = await execute(backtester, make_close_intent(), portfolio, timestamp=T0 + timedelta(hours=1))

        assert portfolio.positions == []
        assert trade.position_id == opened_id
        assert len(portfolio._closed_positions) == 1
        assert portfolio._closed_positions[0].position_id == opened_id

    @pytest.mark.asyncio
    async def test_close_ignores_unmatchable_venue_position_id(self):
        """A venue tradeHash (0x...) never equals a simulated id; matching must
        fall back to market+side instead of silently closing nothing."""
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(), portfolio)
        opened_id = portfolio.positions[0].position_id

        close = make_close_intent(position_id="0x" + "ab" * 32)
        trade = await execute(backtester, close, portfolio, timestamp=T0 + timedelta(hours=1))

        assert portfolio.positions == []
        assert trade.position_id == opened_id

    @pytest.mark.asyncio
    async def test_close_wrong_side_does_not_match(self):
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(is_long=True), portfolio)

        trade = await execute(
            backtester,
            make_close_intent(is_long=False),
            portfolio,
            timestamp=T0 + timedelta(hours=1),
        )

        assert len(portfolio.positions) == 1
        assert trade.position_id is None

    @pytest.mark.asyncio
    async def test_close_wrong_market_does_not_match(self):
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(), portfolio)

        trade = await execute(
            backtester,
            make_close_intent(market="BTC/USD"),
            portfolio,
            timestamp=T0 + timedelta(hours=1),
        )

        assert len(portfolio.positions) == 1
        assert trade.position_id is None

    @pytest.mark.asyncio
    async def test_full_close_amount_is_position_notional(self):
        """size_usd=None means close the whole position; the fee notional is
        the matched position's notional, not zero."""
        backtester = make_backtester(fee_pct=Decimal("0.001"))
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(), portfolio)

        trade = await execute(backtester, make_close_intent(), portfolio, timestamp=T0 + timedelta(hours=1))

        assert trade.amount_usd == Decimal("5000")
        assert trade.fee_usd == Decimal("5")

    @pytest.mark.asyncio
    async def test_close_picks_oldest_matching_position(self):
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(), portfolio, timestamp=T0)
        await execute(
            backtester,
            make_open_intent(),
            portfolio,
            timestamp=T0 + timedelta(hours=1),
        )
        oldest_id = min(portfolio.positions, key=lambda p: p.entry_time).position_id

        trade = await execute(backtester, make_close_intent(), portfolio, timestamp=T0 + timedelta(hours=2))

        assert trade.position_id == oldest_id
        assert len(portfolio.positions) == 1
        assert portfolio.positions[0].position_id != oldest_id

    @pytest.mark.asyncio
    async def test_oversized_close_capped_to_position_notional(self):
        """A close larger than the matched position cannot pay fees on
        notional that does not exist; it caps to the position's notional."""
        backtester = make_backtester(fee_pct=Decimal("0.001"))
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(), portfolio)

        trade = await execute(
            backtester,
            make_close_intent(size_usd=Decimal("10000")),
            portfolio,
            timestamp=T0 + timedelta(hours=1),
        )

        assert portfolio.positions == []
        assert trade.amount_usd == Decimal("5000")
        assert trade.fee_usd == Decimal("5")

    @pytest.mark.asyncio
    async def test_unresolvable_market_close_fails_closed(self):
        """An address-style close market cannot discriminate between open
        positions; refusing to match beats closing the wrong position."""
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(), portfolio)

        trade = await execute(
            backtester,
            make_close_intent(market="0x47c031236e19d024b42f8AE6780E44A573170703"),
            portfolio,
            timestamp=T0 + timedelta(hours=1),
        )

        assert len(portfolio.positions) == 1
        assert trade.position_id is None

    def test_protocol_name_attr_used_for_matching(self):
        """Duck-typed close intents may carry protocol_name instead of
        protocol; matching uses the module's canonical protocol resolver."""
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import find_perp_close_position_id
        from almanak.framework.backtesting.pnl.position_models import SimulatedPosition

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=ETH_PRICE,
            entry_time=T0,
            protocol="gmx_v2",
        )

        matching = SimpleNamespace(market="ETH/USD", is_long=True, protocol_name="gmx_v2")
        assert find_perp_close_position_id(matching, [position]) == position.position_id

        mismatched = SimpleNamespace(market="ETH/USD", is_long=True, protocol_name="hyperliquid")
        assert find_perp_close_position_id(mismatched, [position]) is None

    def test_protocol_scoped_close_ignores_positions_without_protocol(self):
        """A protocol-specific close must not match untagged legacy positions."""
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import find_perp_close_position_id
        from almanak.framework.backtesting.pnl.position_models import SimulatedPosition

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=ETH_PRICE,
            entry_time=T0,
            protocol="gmx_v2",
        )
        position.protocol = None
        intent = SimpleNamespace(market="ETH/USD", is_long=True, protocol="gmx_v2")

        assert find_perp_close_position_id(intent, [position]) is None

    def test_exact_simulated_id_match_takes_precedence(self):
        """Adapter-managed (duck-typed) close intents may carry the simulated
        id directly; an exact match wins before market+side matching."""
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import find_perp_close_position_id
        from almanak.framework.backtesting.pnl.position_models import SimulatedPosition

        eth = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=ETH_PRICE,
            entry_time=T0,
            protocol="gmx_v2",
        )
        btc = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=Decimal("60000"),
            entry_time=T0,
            protocol="gmx_v2",
        )
        # market says ETH, but the explicit id targets the BTC position
        intent = SimpleNamespace(market="ETH/USD", is_long=True, protocol="gmx_v2", position_id=btc.position_id)

        assert find_perp_close_position_id(intent, [eth, btc]) == btc.position_id

    def test_explicit_id_naming_non_perp_fails_closed(self):
        """A malformed PERP_CLOSE must not close a non-perp position by id."""
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import find_perp_close_position_id
        from almanak.framework.backtesting.pnl.position_models import SimulatedPosition

        supply = SimulatedPosition.supply(
            token="ETH",
            amount=Decimal("1"),
            apy=Decimal("0.03"),
            entry_price=ETH_PRICE,
            entry_time=T0,
            protocol="aave_v3",
        )
        intent = SimpleNamespace(market="ETH/USD", is_long=True, protocol="gmx_v2", position_id=supply.position_id)

        assert find_perp_close_position_id(intent, [supply]) is None

    def test_explicit_id_naming_wrong_perp_side_fails_closed(self):
        """A PERP_CLOSE exact-id target must still match the requested side."""
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import find_perp_close_position_id
        from almanak.framework.backtesting.pnl.position_models import SimulatedPosition

        short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=ETH_PRICE,
            entry_time=T0,
            protocol="gmx_v2",
        )
        intent = SimpleNamespace(market="ETH/USD", is_long=True, protocol="gmx_v2", position_id=short.position_id)

        assert find_perp_close_position_id(intent, [short]) is None

    @pytest.mark.asyncio
    async def test_close_creates_no_new_position(self):
        backtester = make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        await execute(backtester, make_open_intent(), portfolio)
        await execute(backtester, make_close_intent(), portfolio, timestamp=T0 + timedelta(hours=1))

        assert portfolio.positions == []
        assert len(portfolio._closed_positions) == 1
