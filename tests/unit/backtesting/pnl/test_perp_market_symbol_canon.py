"""Perp market-symbol canonicalization pins (campaign-50 s42 + s38).

Both engine bugs came from lane-divergent symbol parsing:

- s42 (GMX arbitrum): ``market="ETH/USD"`` — the SDK's own documented slash
  form — priced the perp entry at a silent $1 fallback (unrealized PnL frozen,
  hedge economically inert, "filled" blotter) because the pricing lane could
  not map the base asset onto the run's WETH series.
- s38 (Hyperliquid): the funding lane could not map the slash form onto the
  dash-keyed venue funding tables, so measured funding was only reachable by
  authoring "ETH-USD".

These tests pin the ONE canonicalization seam (``almanak.core.perp_markets``):
slash and dash forms must produce identical entry prices and identical
funding-key resolution, and a genuinely unpriceable market must be a NAMED
rejection — never a $1 entry.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

from almanak.core.perp_markets import perp_market_base, perp_market_funding_key
from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState, token_ref_provider_symbol
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.intent_extraction import (
    get_executed_price,
    resolve_perp_base_price,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from tests.unit.backtesting.pnl._mocks import MockDataProvider

TS = datetime(2026, 6, 16, tzinfo=UTC)
ETH_PRICE = Decimal("3000")
INITIAL_CASH = Decimal("10000")


def _weth_market_state(price: Decimal = ETH_PRICE) -> MarketState:
    """A market state that (like real runs) prices the WRAPPED native only."""
    return MarketState(
        timestamp=TS,
        prices={"WETH": price, "USDC": Decimal("1")},
        chain="arbitrum",
    )


@dataclass
class PerpOpenStub:
    market: str
    intent_type: str = "PERP_OPEN"
    collateral_token: str = "USDC"
    collateral_amount: Decimal = Decimal("2500")
    size_usd: Decimal = Decimal("5000")
    leverage: Decimal = Decimal("2")
    is_long: bool = False
    protocol: str = "gmx_v2"


@dataclass
class PerpCloseStub:
    market: str
    intent_type: str = "PERP_CLOSE"
    is_long: bool = False
    protocol: str = "gmx_v2"


class TestCanonicalParse:
    """The shared parse: every spelling of the same market collapses."""

    @pytest.mark.parametrize(
        "market",
        ["ETH/USD", "ETH-USD", "ETH_USD", "ETH:USD", "ETH", "eth/usd", " ETH/USD "],
    )
    def test_base_is_eth_for_every_spelling(self, market: str) -> None:
        assert perp_market_base(market) == "ETH"

    @pytest.mark.parametrize(
        "market",
        ["ETH/USD", "ETH-USD", "ETH", "eth/usdc"],
    )
    def test_funding_key_is_dash_form_for_every_spelling(self, market: str) -> None:
        assert perp_market_funding_key(market) == "ETH-USD"

    def test_drift_perp_suffix_parses_to_base(self) -> None:
        assert perp_market_base("SOL-PERP") == "SOL"

    @pytest.mark.parametrize("market", ["", "  ", "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336", None, 42])
    def test_unparseable_markets_are_none(self, market: object) -> None:
        assert perp_market_base(market) is None
        assert perp_market_funding_key(market) is None


class TestPerpEntryPricing:
    """Slash and dash forms price identically off the run's WETH series."""

    @pytest.mark.parametrize("market", ["ETH/USD", "ETH-USD"])
    def test_resolve_perp_base_price_maps_native_onto_wrapped(self, market: str) -> None:
        base, priced_symbol, price = resolve_perp_base_price(market, _weth_market_state())
        assert base == "ETH"
        # The native symbol itself is the priceable form: MarketState serves
        # "ETH" through the chain's wrapped-native plane (ALM-2943), so the
        # first candidate resolves and the wrapped-form retry never engages.
        # The price MUST still be the wrapped token's market price.
        assert priced_symbol == "ETH"
        assert price == ETH_PRICE

    def test_slash_and_dash_produce_identical_executed_prices(self) -> None:
        state = _weth_market_state()
        slippage = Decimal("0.001")
        slash = get_executed_price(PerpOpenStub(market="ETH/USD"), state, slippage, IntentType.PERP_OPEN)
        dash = get_executed_price(PerpOpenStub(market="ETH-USD"), state, slippage, IntentType.PERP_OPEN)
        assert slash == dash
        # Short open is a sell: adverse slippage lowers the mark from $3000.
        assert slash == ETH_PRICE * (Decimal("1") - slippage)

    def test_unpriceable_base_is_not_silently_one_dollar_in_resolver(self) -> None:
        base, priced_symbol, price = resolve_perp_base_price("FOO/USD", _weth_market_state())
        assert base == "FOO"
        assert priced_symbol is None
        assert price is None


class TestEnginePerpOpenEndToEnd:
    """Engine-level pins through PnLBacktester._execute_intent (generic lane)."""

    @staticmethod
    def _backtester() -> PnLBacktester:
        return PnLBacktester(
            data_provider=MockDataProvider(),
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

    @staticmethod
    def _config() -> PnLBacktestConfig:
        return PnLBacktestConfig(
            start_time=TS,
            end_time=TS + timedelta(hours=1),
            token_funding=_pnl_token_funding(INITIAL_CASH),
            include_gas_costs=False,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("market", ["ETH/USD", "ETH-USD"])
    async def test_open_fills_with_market_entry_price_and_live_pnl(self, market: str) -> None:
        backtester = self._backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _weth_market_state()

        record = await backtester._execute_intent(PerpOpenStub(market=market), portfolio, state, TS, self._config())

        assert record.success is True
        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        # Entry price is the real market price, not the $1 fallback.
        assert position.entry_price == ETH_PRICE
        # Position keyed by the PRICEABLE form so per-tick marks resolve —
        # "ETH" is priceable directly since MarketState serves native symbols
        # through the wrapped-native plane (ALM-2943).
        assert str(position.tokens[0]).upper() == "ETH"

        # Unrealized PnL is LIVE: a -10% move profits the short by 10% of notional.
        exit_price = ETH_PRICE * Decimal("0.9")
        exit_state = MarketState(
            timestamp=TS + timedelta(hours=1),
            prices={"WETH": exit_price, "USDC": Decimal("1")},
            chain="arbitrum",
        )
        total = portfolio.get_total_value_usd(exit_state)
        assert total == INITIAL_CASH + Decimal("5000") * Decimal("0.1")

    @pytest.mark.asyncio
    async def test_slash_and_dash_produce_identical_positions(self) -> None:
        entries = {}
        for market in ("ETH/USD", "ETH-USD"):
            backtester = self._backtester()
            portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
            await backtester._execute_intent(
                PerpOpenStub(market=market), portfolio, _weth_market_state(), TS, self._config()
            )
            position = portfolio.positions[0]
            entries[market] = (position.entry_price, str(position.tokens[0]).upper(), position.notional_usd)
        assert entries["ETH/USD"] == entries["ETH-USD"]

    @pytest.mark.asyncio
    async def test_unpriceable_market_is_named_rejection_not_one_dollar_entry(self) -> None:
        backtester = self._backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)

        record = await backtester._execute_intent(
            PerpOpenStub(market="FOO/USD"), portfolio, _weth_market_state(), TS, self._config()
        )

        assert record.success is False
        reason = record.metadata.get("failure_reason", "")
        assert "PERP_OPEN market 'FOO/USD'" in reason
        assert "not priceable" in reason
        # Nothing opened, nothing debited: the hedge is visibly absent, not
        # silently inert at a $1 entry.
        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH

    @pytest.mark.asyncio
    async def test_cross_form_close_matches_wrapped_keyed_position(self) -> None:
        """A position opened via 'ETH/USD' (WETH-keyed) closes via 'ETH-USD'."""
        backtester = self._backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _weth_market_state()
        config = self._config()

        await backtester._execute_intent(PerpOpenStub(market="ETH/USD"), portfolio, state, TS, config)
        assert len(portfolio.positions) == 1

        close_record = await backtester._execute_intent(
            PerpCloseStub(market="ETH-USD"), portfolio, state, TS + timedelta(hours=1), config
        )

        assert close_record.success is True
        assert portfolio.positions == []


class TestFundingKeyParity:
    """Funding lanes resolve the same '<BASE>-USD' key for every spelling."""

    def test_wrapped_native_position_token_unwraps_to_funding_symbol(self) -> None:
        # A WETH-keyed position must fund as ETH ("ETH-USD"), matching the
        # prewarm key parsed from the intent's market string.
        assert token_ref_provider_symbol("WETH", "arbitrum", unwrap_wrapped_native=True) == "ETH"

    def test_adapter_funding_lookup_market_for_wrapped_position(self) -> None:
        from almanak.framework.backtesting.adapters.perp_adapter import PerpBacktestAdapter
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        adapter = PerpBacktestAdapter()
        position = SimulatedPosition.perp_short(
            token="WETH",
            collateral_usd=Decimal("2500"),
            leverage=Decimal("2"),
            entry_price=ETH_PRICE,
            entry_time=TS,
            protocol="gmx_v2",
        )
        lookup = adapter._funding_lookup(position, TS, "arbitrum")
        assert lookup.market == "ETH-USD"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("market", ["ETH/USD", "ETH-USD"])
    async def test_snapshot_funding_lane_canonicalizes_market(self, market: str) -> None:
        from almanak.framework.backtesting.pnl.providers.perp.snapshot_funding import (
            SnapshotFundingRateSource,
        )

        source = SnapshotFundingRateSource(chain="arbitrum")
        rate = await source.funding_rate_at("hyperliquid", market, TS)
        assert rate.market == "ETH-USD"

    @pytest.mark.asyncio
    async def test_snapshot_funding_slash_and_dash_share_one_cache_entry(self) -> None:
        from almanak.framework.backtesting.pnl.providers.perp.snapshot_funding import (
            SnapshotFundingRateSource,
        )

        source = SnapshotFundingRateSource(chain="arbitrum")
        slash = await source.funding_rate_at("hyperliquid", "ETH/USD", TS)
        dash = await source.funding_rate_at("hyperliquid", "ETH-USD", TS)
        assert slash == dash
        assert len(source._cache) == 1
