"""Address-first perp fill pricing against the verified market catalog.

An address-first GMX strategy authors the venue market-token ADDRESS. The
fill-pricing lane resolves its index base symbol through
``PerpsReadRegistry.market_metadata`` → the connector's process-wide verified
market catalog. That catalog is populated by the intent compiler's dynamic
gateway verification on the live path and by the candle-lane provider's
``prime_market_catalog`` at PnL-backtest startup — a PnL backtest never
compiles, so before priming existed every address-form PERP_OPEN was rejected
as unpriceable (1105 attempts, 0 fills). These tests pin both sides of the
fail-closed contract:

- primed catalog → the open fills at the real market price with a
  price-tracked position and live PnL;
- unprimed catalog → the NAMED rejection stands (never a $1 entry, never a
  guessed symbol).
"""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.connectors.gmx_v2 import market_catalog
from almanak.connectors.gmx_v2.market_metadata import ResolvedGmxMarket
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.intent_extraction import resolve_perp_base_price
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding
from tests.unit.backtesting.pnl._mocks import MockDataProvider

TS = datetime(2026, 6, 16, tzinfo=UTC)
ETH_PRICE = Decimal("3000")
INITIAL_CASH = Decimal("10000")

# The real GMX arbitrum ETH/USD market token (the repro's rejected address).
MARKET_ADDRESS = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

# A second verified ETH-index collateral variant (GMX lists several market
# tokens per index, e.g. the single-sided WETH-WETH market). Same base symbol
# "ETH" as MARKET_ADDRESS — the close-selection test below exists precisely
# because a symbol-level match cannot tell these two markets apart.
ETH_VARIANT_MARKET_ADDRESS = "0x450bb6774Dd8a756274E0ab4107953259d2ac541"


@pytest.fixture(autouse=True)
def _clean_catalog() -> Iterator[None]:
    market_catalog.clear()
    yield
    market_catalog.clear()


def _prime_catalog() -> None:
    market_catalog.remember(
        "arbitrum",
        ResolvedGmxMarket(
            label="ETH/USD [WETH-USDC]",
            market_token=MARKET_ADDRESS,
            index_token=WETH_ARBITRUM,
            index_symbol="ETH",
            index_token_decimals=18,
            long_token=WETH_ARBITRUM,
            long_token_symbol="WETH",
            short_token=USDC_ARBITRUM,
            short_token_symbol="USDC",
        ),
    )


def _prime_eth_variant_catalog() -> None:
    market_catalog.remember(
        "arbitrum",
        ResolvedGmxMarket(
            label="ETH/USD [WETH-WETH]",
            market_token=ETH_VARIANT_MARKET_ADDRESS,
            index_token=WETH_ARBITRUM,
            index_symbol="ETH",
            index_token_decimals=18,
            long_token=WETH_ARBITRUM,
            long_token_symbol="WETH",
            short_token=WETH_ARBITRUM,
            short_token_symbol="WETH",
        ),
    )


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


class TestResolverSeam:
    """``resolve_perp_base_price`` is catalog-gated for address-form markets."""

    def test_unprimed_catalog_resolves_nothing(self) -> None:
        base, priced_symbol, price = resolve_perp_base_price(MARKET_ADDRESS, _weth_market_state(), protocol="gmx_v2")
        assert base is None
        assert priced_symbol is None
        assert price is None

    def test_primed_catalog_resolves_verified_index_symbol(self) -> None:
        _prime_catalog()
        base, priced_symbol, price = resolve_perp_base_price(MARKET_ADDRESS, _weth_market_state(), protocol="gmx_v2")
        assert base == "ETH"
        assert priced_symbol == "ETH"
        assert price == ETH_PRICE


class TestEngineAddressFirstFill:
    """Engine-level pins through ``PnLBacktester._execute_intent``."""

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
    async def test_primed_open_fills_price_tracked_with_live_pnl(self) -> None:
        _prime_catalog()
        backtester = self._backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)

        record = await backtester._execute_intent(
            PerpOpenStub(market=MARKET_ADDRESS), portfolio, _weth_market_state(), TS, self._config()
        )

        assert record.success is True
        assert len(portfolio.positions) == 1
        position = portfolio.positions[0]
        # Entry at the real market price, keyed by the priceable base symbol
        # so per-tick marks resolve (not the UNKNOWN sentinel, not $1).
        assert position.entry_price == ETH_PRICE
        assert str(position.tokens[0]).upper() == "ETH"

        # Unrealized PnL is LIVE: a -10% move profits the short by 10% of notional.
        exit_state = MarketState(
            timestamp=TS + timedelta(hours=1),
            prices={"WETH": ETH_PRICE * Decimal("0.9"), "USDC": Decimal("1")},
            chain="arbitrum",
        )
        total = portfolio.get_total_value_usd(exit_state)
        assert total == INITIAL_CASH + Decimal("5000") * Decimal("0.1")

    @pytest.mark.asyncio
    async def test_primed_close_matches_the_exact_market_token_not_fifo_by_symbol(self) -> None:
        """An address-form close targets the exact market token, never a sibling.

        GMX lists several collateral-variant market tokens per index; both
        variants here resolve to base "ETH", so a symbol-level `(base, side,
        protocol)` match cannot tell them apart and FIFO would close whichever
        was opened FIRST. The close names the SECOND-opened variant's address
        and must remove exactly that position — matched against the market
        identity the open path stamped — leaving the older sibling untouched
        (PR #3660 review, lars0x).
        """
        _prime_catalog()
        _prime_eth_variant_catalog()
        backtester = self._backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _weth_market_state()
        config = self._config()

        await backtester._execute_intent(PerpOpenStub(market=ETH_VARIANT_MARKET_ADDRESS), portfolio, state, TS, config)
        await backtester._execute_intent(
            PerpOpenStub(market=MARKET_ADDRESS), portfolio, state, TS + timedelta(minutes=5), config
        )
        assert len(portfolio.positions) == 2

        close_record = await backtester._execute_intent(
            PerpCloseStub(market=MARKET_ADDRESS), portfolio, state, TS + timedelta(hours=1), config
        )

        assert close_record.success is True
        assert len(portfolio.positions) == 1
        remaining = portfolio.positions[0]
        assert remaining.metadata.get("perp_market") == ETH_VARIANT_MARKET_ADDRESS
        assert remaining.entry_time == TS

    @pytest.mark.asyncio
    async def test_label_opened_position_still_closes_by_address(self) -> None:
        """The stamped-address match must not strand label-opened positions.

        A position opened via the pair label carries the label as its stamped
        market, so an address-form close finds no exact-address match and
        must fall back to the symbol matcher (base, side, protocol) — the
        cross-form compatibility the resolver seam guarantees.
        """
        _prime_catalog()
        backtester = self._backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _weth_market_state()
        config = self._config()

        await backtester._execute_intent(PerpOpenStub(market="ETH/USD"), portfolio, state, TS, config)
        assert len(portfolio.positions) == 1

        close_record = await backtester._execute_intent(
            PerpCloseStub(market=MARKET_ADDRESS), portfolio, state, TS + timedelta(hours=1), config
        )

        assert close_record.success is True
        assert portfolio.positions == []

    @pytest.mark.asyncio
    async def test_unprimed_open_is_named_rejection_not_silent_entry(self) -> None:
        backtester = self._backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)

        record = await backtester._execute_intent(
            PerpOpenStub(market=MARKET_ADDRESS), portfolio, _weth_market_state(), TS, self._config()
        )

        assert record.success is False
        reason = record.metadata.get("failure_reason", "")
        assert f"PERP_OPEN market '{MARKET_ADDRESS}'" in reason
        assert "not priceable" in reason
        # Nothing opened, nothing debited: the failure is visible, not a
        # silently inert position at a fabricated entry price.
        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
