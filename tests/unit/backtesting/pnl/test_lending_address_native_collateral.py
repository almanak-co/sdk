"""BORROW health-factor collateral on address-native market state.

Post-VIB-5508 the PnL engine keys ``MarketState.prices`` by
``(chain, address)`` and plain-symbol reads are an honest miss. The
2026-07 symbol alias bridge restored the strategy-facing surface
(``MarketSnapshot``), but engine-internal reads still consumed the plain
symbols carried by intents: ``get_intent_amount_usd`` sized a
``SupplyIntent(token="USDC")`` at $0 ("no price" fallback), the supply
position landed with ``total_amount=0``, and the lending adapter's BORROW
health check (``_get_total_collateral_value`` -> ``value_position``)
computed Collateral=$0.00 -> HF=0.0000 -> every BORROW rejected, even
though the SUPPLY had "executed" (as a $0 economic no-op that never
debited cash).

The fix is the ``MarketState`` analogue of the snapshot bridge:
``MarketState.register_symbol_aliases`` registers the run's own
``{SYMBOL: (chain, address)}`` map as read aliases (never guessed;
cross-chain and unregistered symbols stay honest misses; state keys stay
address-native), and ``execute_iteration_loop`` registers the map on
every tick's market state. Blueprint 31 §2 documents the design.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.adapters.lending_adapter import LendingBacktestAdapter
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.portfolio import PositionType, SimulatedPortfolio
from almanak.framework.intents.lending_intents import BorrowIntent, SupplyIntent
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding
from tests.validation.backtesting.trust_matrix import (
    START,
    ScriptedStrategy,
    SyntheticPriceProvider,
)

CHAIN = "arbitrum"
USDC_ADDRESS = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
USDT_ADDRESS = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"
USDC_KEY = (CHAIN, USDC_ADDRESS)
USDT_KEY = (CHAIN, USDT_ADDRESS)
TOKEN_ADDRESSES = {"USDC": USDC_KEY, "USDT": USDT_KEY}

TS = datetime(2026, 5, 1, tzinfo=UTC)
INITIAL_CASH = Decimal("1250")
SUPPLY_AMOUNT = Decimal("400")
BORROW_AMOUNT = Decimal("80")


def address_native_state(timestamp: datetime = TS, *, aliases: bool = True) -> MarketState:
    """Production-shaped market state: prices keyed by ``(chain, address)``."""
    state = MarketState(
        timestamp=timestamp,
        prices={USDC_KEY: Decimal("1"), USDT_KEY: Decimal("1")},
        chain=CHAIN,
    )
    if aliases:
        state.register_symbol_aliases(TOKEN_ADDRESSES)
    return state


def supply_intent() -> SupplyIntent:
    return SupplyIntent(protocol="aave_v3", token="USDC", amount=SUPPLY_AMOUNT, chain=CHAIN)


def borrow_intent() -> BorrowIntent:
    return BorrowIntent(
        protocol="aave_v3",
        collateral_token="USDC",
        collateral_amount=Decimal("0"),
        borrow_token="USDT",
        borrow_amount=BORROW_AMOUNT,
        chain=CHAIN,
    )


def _backtester() -> PnLBacktester:
    backtester = PnLBacktester(
        data_provider=SyntheticPriceProvider({}),
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )
    backtester._adapter = LendingBacktestAdapter()
    return backtester


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=TS,
        end_time=TS + timedelta(hours=1),
        token_funding=_pnl_token_funding(INITIAL_CASH, chain=CHAIN),
        include_gas_costs=False,
    )


class TestMarketStateSymbolAliases:
    """Read-alias semantics on MarketState (mirrors the snapshot bridge pins)."""

    def test_registered_symbol_resolves_to_address_native_price(self) -> None:
        state = address_native_state()
        assert state.get_price("USDC") == Decimal("1")
        assert state.has_token("usdt")

    def test_unregistered_symbol_stays_honest_miss(self) -> None:
        state = address_native_state()
        with pytest.raises(KeyError):
            state.get_price("WETH")

    def test_without_registration_symbol_read_stays_honest_miss(self) -> None:
        state = address_native_state(aliases=False)
        with pytest.raises(KeyError):
            state.get_price("USDC")

    def test_cross_chain_entries_are_not_aliased(self) -> None:
        state = address_native_state(aliases=False)
        state.register_symbol_aliases({"USDC": ("polygon", USDC_ADDRESS)})
        with pytest.raises(KeyError):
            state.get_price("USDC")

    def test_literal_symbol_fixture_key_wins_over_alias(self) -> None:
        state = address_native_state()
        state.prices["USDC"] = Decimal("2")
        assert state.get_price("USDC") == Decimal("2")

    def test_non_token_key_entries_are_ignored(self) -> None:
        state = address_native_state(aliases=False)
        state.register_symbol_aliases({"USDC": "not-a-key"})
        with pytest.raises(KeyError):
            state.get_price("USDC")

    SOL_MINT = "So11111111111111111111111111111111111111112"
    SOL_KEY = ("solana", SOL_MINT)

    def _solana_state(self) -> MarketState:
        return MarketState(
            timestamp=TS,
            prices={self.SOL_KEY: Decimal("150")},
            chain="solana",
        )

    def test_non_evm_symbol_alias_resolves(self) -> None:
        # Base58 mints are case-sensitive: the alias must preserve casing.
        state = self._solana_state()
        state.register_symbol_aliases({"SOL": self.SOL_KEY})
        assert state.get_price("SOL") == Decimal("150")

    def test_available_tokens_round_trip_through_get_price(self) -> None:
        # The engine loop reads available_tokens display keys back through
        # get_price() for indicator population; the round trip must hold for
        # non-EVM (chain, mint) keys too, not just EVM display parses
        # (PR #3156 review).
        for state in (address_native_state(aliases=False), self._solana_state()):
            for display in state.available_tokens:
                assert state.get_price(display) > 0, display


class TestBorrowHealthCheckAddressNativeCollateral:
    """The VIB-5508-class regression: SUPPLY collateral visible to BORROW HF."""

    @pytest.mark.asyncio
    async def test_supply_position_produces_nonzero_collateral_in_borrow_health_check(self) -> None:
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(cash_usd=INITIAL_CASH, chain=CHAIN)

        supply_trade = await backtester._execute_intent(
            supply_intent(), portfolio, address_native_state(), TS, config
        )
        assert supply_trade.success
        # The supply is a real economic event, not a $0 no-op: it is sized
        # at the address-native price and debits cash.
        assert supply_trade.amount_usd == SUPPLY_AMOUNT
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT
        supply_positions = [p for p in portfolio.positions if p.position_type == PositionType.SUPPLY]
        assert len(supply_positions) == 1
        assert supply_positions[0].total_amount == SUPPLY_AMOUNT

        borrow_state = address_native_state(TS + timedelta(hours=1))
        adapter = backtester._adapter
        assert isinstance(adapter, LendingBacktestAdapter)
        collateral = adapter._get_total_collateral_value(portfolio, borrow_state)
        assert collateral == SUPPLY_AMOUNT

        borrow_trade = await backtester._execute_intent(
            borrow_intent(), portfolio, borrow_state, TS + timedelta(hours=1), config
        )
        assert borrow_trade.success
        assert borrow_trade.metadata.get("failure_reason") is None
        borrow_positions = [p for p in portfolio.positions if p.position_type == PositionType.BORROW]
        assert len(borrow_positions) == 1
        assert borrow_positions[0].total_amount == BORROW_AMOUNT

    @pytest.mark.asyncio
    async def test_unbridged_state_rejects_borrow_with_zero_collateral(self) -> None:
        """Documents the failure mode the alias bridge closes: on a raw
        address-keyed state (no registered aliases) the plain-symbol supply
        is sized at $0 and the BORROW health check sees no collateral."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(cash_usd=INITIAL_CASH, chain=CHAIN)

        await backtester._execute_intent(
            supply_intent(), portfolio, address_native_state(aliases=False), TS, config
        )
        borrow_trade = await backtester._execute_intent(
            borrow_intent(),
            portfolio,
            address_native_state(TS + timedelta(hours=1), aliases=False),
            TS + timedelta(hours=1),
            config,
        )
        assert not borrow_trade.success
        assert borrow_trade.metadata.get("failure_reason") == "Health factor would be below 1.0"

    def test_full_engine_loop_bridges_lending_lifecycle(self) -> None:
        """The loop registers the run's token-address map on every tick's
        market state, so a plain-symbol SUPPLY -> BORROW lifecycle executes
        against an address-native provider (the shape of the benqi demo on
        a real CoinGecko-backed run)."""
        hours = 4
        series = {
            USDC_KEY: [Decimal("1")] * (hours + 1),
            USDT_KEY: [Decimal("1")] * (hours + 1),
        }
        config = PnLBacktestConfig(
            start_time=START,
            end_time=START + timedelta(hours=hours),
            interval_seconds=3600,
            token_funding=[
                {
                    "symbol": "USDC",
                    "address": USDC_ADDRESS,
                    "chain": CHAIN,
                    "amount": str(INITIAL_CASH),
                    "amount_type": "token",
                }
            ],
            tokens=list(series),
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )
        backtester = PnLBacktester(
            data_provider=SyntheticPriceProvider(series),
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
            strategy_type="lending",
            token_addresses=dict(TOKEN_ADDRESSES),
        )
        strategy = ScriptedStrategy([supply_intent(), borrow_intent()])

        result = asyncio.run(backtester.backtest(strategy, config))

        assert len(result.trades) == 2
        for trade in result.trades:
            assert trade.success, trade.metadata.get("failure_reason")
        supply_trade, borrow_trade = result.trades
        assert supply_trade.amount_usd == SUPPLY_AMOUNT
        assert borrow_trade.amount_usd == BORROW_AMOUNT
        assert borrow_trade.metadata.get("failure_reason") is None
