"""Native gas assets are valuable and readable address-natively (ALM-3067).

A live-traded strategy produced 0 trades in a staging backtest because
`market.balance("ETH")` raised on 2161/2161 ticks; its guard turned every
failure into HOLD, and the run still reported -8.08% / Sharpe -1.24 (funded
WETH depreciating while the strategy sat inert).

Two defects stacked:

1. `MarketState._lookup_keys` implemented the chain registry's native<->wrapped
   1:1 equivalence (blueprint 31 §2) only on the *symbol* branch. The
   `is_token_key` branch returned early, so `get_price("ETH")` resolved through
   wrapped but `get_price(("arbitrum", <sentinel>))` raised -- i.e. the only
   token form SDK 3.0 accepts for a native asset could not be valued at all.
2. Because natives could not be valued address-natively,
   `build_backtest_token_address_map` excluded them. That map also drives the
   balance seeder and the symbol-alias bridges, so the balance lane starved.

These tests pin both fixes, and pin the two things the fix must NOT do:
fabricate a price for an unpriced token, and collapse native onto wrapped in
an identity map (ALM-3058's defect).
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_provider import (
    MarketState,
    native_token_map_entry,
    normalize_token_key,
)
from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

ARB_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
ARB_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
SENTINEL = NATIVE_SENTINEL.lower()
TS = datetime(2026, 7, 29, 12, tzinfo=UTC)

# The run's registered identity map as `build_backtest_token_address_map` now
# emits it: the native registered at the sentinel, distinct from wrapped.
TOKEN_ADDRESSES = {
    "WETH": ("arbitrum", ARB_WETH),
    "USDC": ("arbitrum", ARB_USDC),
    "ETH": ("arbitrum", SENTINEL),
}


def _state_with_wrapped_priced(price: str = "3000") -> MarketState:
    """MarketState as the CoinGecko address lane actually produces it.

    The wrapped ERC-20 is priced under its `(chain, address)` key; the native
    gas asset has no entry of its own.
    """
    state = MarketState(
        timestamp=TS,
        chain="arbitrum",
        prices={("arbitrum", ARB_WETH): Decimal(price)},
    )
    state.register_symbol_aliases({"WETH": ("arbitrum", ARB_WETH)})
    return state


class TestNativeWrappedBridgeOnAddressNativePath:
    def test_native_sentinel_key_prices_through_wrapped(self):
        """The regression: this raised KeyError before the fix."""
        state = _state_with_wrapped_priced()

        assert state.get_price(normalize_token_key("arbitrum", SENTINEL)) == Decimal("3000")

    def test_native_symbol_and_native_sentinel_agree(self):
        """Symbol and address-native forms of the same asset must not diverge."""
        state = _state_with_wrapped_priced()

        assert state.get_price("ETH") == state.get_price(normalize_token_key("arbitrum", SENTINEL))

    def test_checksummed_sentinel_resolves_like_lowercase(self):
        state = _state_with_wrapped_priced()

        assert state.get_price(("arbitrum", NATIVE_SENTINEL)) == Decimal("3000")

    def test_directly_priced_native_keeps_precedence_over_wrapped(self):
        """A real native entry wins; the wrapped fallback is last-resort only.

        Native and wrapped are 1:1 by peg, not by definition -- if the run
        actually priced the native, that number is authoritative.
        """
        state = _state_with_wrapped_priced(price="3000")
        state.prices[("arbitrum", SENTINEL)] = Decimal("2999")

        assert state.get_price(normalize_token_key("arbitrum", SENTINEL)) == Decimal("2999")


class TestBridgeDoesNotFabricatePrices:
    def test_unpriced_non_native_address_stays_an_honest_miss(self):
        """The fallback must be native-only, never a general 'use any price'."""
        state = _state_with_wrapped_priced()

        with pytest.raises(KeyError):
            state.get_price(("arbitrum", ARB_USDC))

    def test_cross_chain_native_stays_a_miss(self):
        """A native sentinel on another chain is a different asset."""
        state = _state_with_wrapped_priced()

        with pytest.raises(KeyError):
            state.get_price(normalize_token_key("polygon", SENTINEL))

    def test_native_unresolvable_when_wrapped_is_also_unpriced(self):
        """No wrapped price means no native price. No fabrication."""
        state = MarketState(timestamp=TS, chain="arbitrum", prices={})

        with pytest.raises(KeyError):
            state.get_price(normalize_token_key("arbitrum", SENTINEL))


class TestNativeStaysDistinctFromWrapped:
    def test_native_and_wrapped_are_separate_keys(self):
        """ALM-3058 guard: the bridge is a price fallback, not an identity merge."""
        state = _state_with_wrapped_priced()
        state.prices[("arbitrum", SENTINEL)] = Decimal("2999")

        assert state.get_price(("arbitrum", ARB_WETH)) == Decimal("3000")
        assert state.get_price(("arbitrum", SENTINEL)) == Decimal("2999")


def _tick_snapshot(portfolio: SimulatedPortfolio):
    """A snapshot as one backtest tick builds it, aliases registered."""
    state = _state_with_wrapped_priced()
    state.register_symbol_aliases(TOKEN_ADDRESSES)
    return create_market_snapshot_from_state(
        state,
        chain="arbitrum",
        portfolio=portfolio,
        token_addresses=TOKEN_ADDRESSES,
    )


def _portfolio(**tokens: str) -> SimulatedPortfolio:
    portfolio = SimulatedPortfolio(
        initial_capital_usd=Decimal("1000"),
        cash_usd=Decimal("500"),
        chain="arbitrum",
    )
    for address, amount in tokens.items():
        portfolio.tokens[("arbitrum", address)] = Decimal(amount)
    return portfolio


class TestTickCanAnswerNativeBalance:
    """End-to-end through the snapshot a strategy actually reads on a tick.

    The unit tests above pin the price plane; these pin the behaviour ALM-3067
    is actually about -- a `decide()` call reading `market.balance("ETH")`.
    """

    def test_unfunded_native_answers_zero_instead_of_raising(self):
        """THE regression: this raised `Cannot determine balance for ETH@arbitrum`.

        The reproduction shape: WETH funded, native never funded. The wallet
        genuinely holds no ETH, so zero is the measured answer -- the strategy
        gets to decide, rather than holding blind on an exception.
        """
        snapshot = _tick_snapshot(_portfolio(**{ARB_WETH: "0.075"}))

        assert snapshot.balance("ETH").balance == Decimal("0")

    def test_unfunded_native_records_no_decision_input_failure(self):
        """A HOLD after this tick is a real choice, not a starved input."""
        snapshot = _tick_snapshot(_portfolio(**{ARB_WETH: "0.075"}))
        snapshot.balance("ETH")

        assert dict(snapshot._critical_data_failures) == {}

    def test_funded_native_reads_its_real_amount_and_usd(self):
        """The zero-seed must not clobber a native the portfolio does hold."""
        snapshot = _tick_snapshot(_portfolio(**{ARB_WETH: "0.075", SENTINEL: "1.5"}))

        balance = snapshot.balance("ETH")
        assert balance.balance == Decimal("1.5")
        assert balance.balance_usd == Decimal("4500.0")

    def test_held_wrapped_balance_survives_the_widened_zero_seed(self):
        snapshot = _tick_snapshot(_portfolio(**{ARB_WETH: "0.075"}))

        assert snapshot.balance("WETH").balance == Decimal("0.075")

    def test_unregistered_symbol_is_still_an_honest_miss(self):
        """Zero-seeding is registered-only -- it must not answer for anything."""
        snapshot = _tick_snapshot(_portfolio(**{ARB_WETH: "0.075"}))

        with pytest.raises(ValueError):
            snapshot.balance("PEPE")


class TestWidenedZeroSeedPreservesCash:
    """Zero-seeding now covers registered tokens, not just priced ones.

    A registered stablecoin the portfolio does not hold is a cash-observation
    key (blueprint 31 §4.1), so widening the zero-seed set must not turn it
    into a zero and must not double-count cash in the portfolio total.
    """

    def test_registered_stablecoin_still_observes_cash(self):
        snapshot = _tick_snapshot(_portfolio(**{ARB_WETH: "0.075"}))

        assert snapshot.balance("USDC").balance == Decimal("500")
        assert snapshot.balance("USD").balance == Decimal("500")

    def test_cash_is_counted_once_not_once_per_mirror_key(self):
        """500 cash + 0.075 WETH @ 3000 = 725, with USD/USDC mirroring the same cash."""
        snapshot = _tick_snapshot(_portfolio(**{ARB_WETH: "0.075"}))

        assert snapshot.total_portfolio_usd() == Decimal("725.000")


class TestNativeCoinIdResolvesRegistrySide:
    """The data-fetch lane must not send the sentinel to the contract endpoint.

    `_resolve_token_id` routes every `(chain, address)` ref straight to
    `/coins/{platform}/contract/{address}`. CoinGecko has no listing for the
    native sentinel, so a `token_funding` entry declaring the native was an
    unpriceable tracked token: the run aborted at preflight
    (`token_availability`), or in degraded mode died at `_funding_price` with
    "Missing first-tick price for funded token ETH". Both reproduced on a real
    CoinGecko-backed backtest before this fix (ALM-3067).
    """

    def test_sentinel_resolves_to_the_chains_native_coin_id(self):
        from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider

        assert CoinGeckoDataProvider._native_coin_id_for_address("arbitrum", SENTINEL) == "ethereum"

    def test_sentinel_is_case_insensitive(self):
        from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider

        assert CoinGeckoDataProvider._native_coin_id_for_address("arbitrum", NATIVE_SENTINEL) == "ethereum"

    def test_native_id_is_per_chain_not_hardcoded(self):
        from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider

        assert CoinGeckoDataProvider._native_coin_id_for_address("polygon", SENTINEL) == "polygon-ecosystem-token"

    def test_wrapped_native_still_resolves_by_contract(self):
        """Wrapped natives must keep the contract endpoint.

        Its chain-specific (bridged) coin id is deliberately the asset actually
        traded; short-circuiting them to the canonical id would price a
        different asset.
        """
        from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider

        assert CoinGeckoDataProvider._native_coin_id_for_address("arbitrum", ARB_WETH) is None

    def test_unknown_chain_is_an_honest_miss(self):
        from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider

        assert CoinGeckoDataProvider._native_coin_id_for_address("nosuchchain", SENTINEL) is None


class TestNativeAlwaysInTokenMap:
    """The run chain's native gas asset is always a registered token (ALM-3067).

    Registering natives was necessary but not sufficient: nothing SUPPLIED the
    native ref unless the user funded or tracked it, and the staging strategy
    (fund WETH+USDC, then guard the gas reserve via ``market.balance("ETH")``)
    does neither. Both map builders merge ``native_token_map_entry`` so the
    native is part of the universe unconditionally.
    """

    def test_evm_chain_yields_native_at_sentinel(self):
        assert native_token_map_entry("arbitrum") == ("ETH", ("arbitrum", SENTINEL))

    def test_symbol_is_per_chain_not_hardcoded(self):
        symbol, key = native_token_map_entry("polygon")
        assert symbol == "MATIC"
        assert key == ("polygon", SENTINEL)

    def test_non_evm_chain_is_refused(self):
        """The sentinel is an EVM convention; solana must not get one."""
        assert native_token_map_entry("solana") is None

    def test_unknown_chain_is_refused(self):
        assert native_token_map_entry("nosuchchain") is None

    def test_cli_map_builder_registers_native_unprompted(self):
        """The CLI lane's Refinement-R1 native exclusion must not survive:
        this map also drives the balance seeder and the alias bridges."""
        from almanak.framework.cli.backtest.run_helpers import build_token_address_map

        strategy_config = {
            "token_funding": [
                {
                    "symbol": "WETH",
                    "address": ARB_WETH,
                    "chain": "arbitrum",
                    "amount": "1",
                    "amount_type": "token",
                },
            ]
        }
        address_map = build_token_address_map(strategy_config, ["WETH", "USDC"], "arbitrum")

        assert address_map["ETH"] == ("arbitrum", SENTINEL)
        assert address_map["WETH"] == ("arbitrum", ARB_WETH)
        assert address_map["ETH"] != address_map["WETH"]
