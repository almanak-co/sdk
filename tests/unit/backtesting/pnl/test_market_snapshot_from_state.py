"""Tests for create_market_snapshot_from_state balance exposure."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state


def _make_market_state(tokens: dict[str, Decimal], timestamp: datetime | None = None):
    """Create a minimal MarketState mock with given token prices."""
    state = MagicMock()
    state.timestamp = timestamp or datetime(2025, 1, 15, 12, 0, tzinfo=UTC)
    state.available_tokens = list(tokens.keys())

    def get_price(token):
        if token in tokens:
            return tokens[token]
        raise KeyError(f"No price for {token}")

    state.get_price = get_price
    return state


def _make_portfolio(cash_usd: Decimal, tokens: dict[str, Decimal] | None = None):
    """Create a minimal SimulatedPortfolio mock."""
    portfolio = MagicMock()
    portfolio.cash_usd = cash_usd
    portfolio.tokens = tokens or {}
    return portfolio


class TestStablecoinAliases:
    """Verify that cash_usd is exposed under stablecoin symbols."""

    def test_usdc_returns_cash_balance(self):
        state = _make_market_state({"WETH": Decimal("3000"), "USDC": Decimal("1")})
        portfolio = _make_portfolio(cash_usd=Decimal("10000"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("USDC")
        assert bal.balance == Decimal("10000")
        assert bal.balance_usd == Decimal("10000")

    def test_usdt_returns_cash_balance(self):
        state = _make_market_state({"WETH": Decimal("3000")})
        portfolio = _make_portfolio(cash_usd=Decimal("5000"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("USDT")
        assert bal.balance == Decimal("5000")

    def test_dai_returns_cash_balance(self):
        state = _make_market_state({"WETH": Decimal("3000")})
        portfolio = _make_portfolio(cash_usd=Decimal("2500"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("DAI")
        assert bal.balance == Decimal("2500")

    def test_stablecoin_alias_does_not_override_real_holding(self):
        """If portfolio holds actual USDC tokens, the alias should not override."""
        state = _make_market_state({"USDC": Decimal("1")})
        portfolio = _make_portfolio(
            cash_usd=Decimal("10000"),
            tokens={"USDC": Decimal("500")},
        )
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("USDC")
        # Should use the actual token holding (500 USDC), not the cash alias
        assert bal.balance == Decimal("500")

    def test_usdc_address_returns_cash_balance(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        usdc_addr = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        state = MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={("base", usdc_addr): Decimal("1")},
        )
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")

        snapshot = create_market_snapshot_from_state(state, chain="base", portfolio=portfolio)

        assert snapshot.price(usdc_addr) == Decimal("1")
        bal = snapshot.balance(usdc_addr)
        assert bal.balance == Decimal("10000")
        assert bal.balance_usd == Decimal("10000")


class TestZeroBalanceForTrackedTokens:
    """Verify that tracked tokens not in portfolio get zero balances."""

    def test_tracked_token_returns_zero_balance(self):
        state = _make_market_state({"WETH": Decimal("3000"), "WBTC": Decimal("60000")})
        portfolio = _make_portfolio(cash_usd=Decimal("10000"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("WETH")
        assert bal.balance == Decimal("0")
        assert bal.balance_usd == Decimal("0")

    def test_held_token_not_overridden_by_zero(self):
        """Tokens actually held in portfolio should keep their real balance."""
        state = _make_market_state({"WETH": Decimal("3000")})
        portfolio = _make_portfolio(
            cash_usd=Decimal("5000"),
            tokens={"WETH": Decimal("2.5")},
        )
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("WETH")
        assert bal.balance == Decimal("2.5")

    def test_held_token_without_price_keeps_amount_with_zero_usd(self):
        """Missing prices for held balances are exposed as unvalued, not dropped."""
        state = _make_market_state({"WETH": Decimal("3000")})
        portfolio = _make_portfolio(
            cash_usd=Decimal("5000"),
            tokens={"ARB": Decimal("100")},
        )
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("ARB")
        assert bal.balance == Decimal("100")
        assert bal.balance_usd == Decimal("0")

    def test_multiple_tracked_tokens_all_zero(self):
        state = _make_market_state(
            {
                "WETH": Decimal("3000"),
                "WBTC": Decimal("60000"),
                "ARB": Decimal("1.5"),
            }
        )
        portfolio = _make_portfolio(cash_usd=Decimal("10000"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        for token in ["WETH", "WBTC", "ARB"]:
            bal = snapshot.balance(token)
            assert bal.balance == Decimal("0"), f"{token} should have zero balance"


class TestNoPortfolio:
    """Verify behavior when no portfolio is provided."""

    def test_no_portfolio_no_balance_error(self):
        state = _make_market_state({"WETH": Decimal("3000")})
        snapshot = create_market_snapshot_from_state(state)

        # Without a portfolio, no balances are set; balance() should raise
        with pytest.raises(ValueError, match="Cannot determine balance"):
            snapshot.balance("WETH")

    def test_prices_still_available_without_portfolio(self):
        state = _make_market_state({"WETH": Decimal("3000")})
        snapshot = create_market_snapshot_from_state(state)

        price = snapshot.price("WETH")
        assert price == Decimal("3000")


class TestAddressKeyedSnapshotResolution:
    """A strategy may reference tokens by contract address without an alias map."""

    CB_ADDR = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
    USDC_ADDR = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    CB_KEY = ("base", CB_ADDR)
    USDC_KEY = ("base", USDC_ADDR)

    def _real_state(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        return MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={self.CB_KEY: Decimal("60000"), self.USDC_KEY: Decimal("1")},
        )

    def test_marketstate_get_price_resolves_address_any_case(self):
        state = self._real_state()
        assert state.get_price(self.CB_ADDR) == Decimal("60000")
        assert state.get_price(self.CB_ADDR.upper()) == Decimal("60000")  # fill-pricing keys uppercase
        assert state.get_price(f"base:{self.CB_ADDR}") == Decimal("60000")

    def test_marketstate_unmapped_address_is_honest_miss(self):
        state = self._real_state()
        with pytest.raises(KeyError):
            state.get_price("0xdeadbeef00000000000000000000000000000000")

    def test_marketstate_symbol_is_honest_miss_for_address_keyed_state(self):
        state = self._real_state()
        with pytest.raises(KeyError):
            state.get_price("CBBTC")

    def test_snapshot_price_and_balance_resolve_by_address(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        state = self._real_state()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        portfolio.tokens[self.CB_KEY] = Decimal("0.05")
        snapshot = create_market_snapshot_from_state(state, chain="base", portfolio=portfolio)
        assert snapshot.price(self.CB_ADDR) == snapshot.price(f"base:{self.CB_ADDR}") == Decimal("60000")
        assert snapshot.balance(self.CB_ADDR).balance == Decimal("0.05")

    def test_tuple_tracked_token_gets_string_keyed_zero_balance(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        state = MagicMock()
        state.timestamp = datetime(2026, 3, 23, tzinfo=UTC)
        state.chain = "base"
        state.prices = {self.CB_KEY: Decimal("60000")}
        state.ohlcv = {}
        state.available_tokens = [self.CB_KEY]
        state.get_price.side_effect = lambda token: state.prices[token]
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")

        snapshot = create_market_snapshot_from_state(state, chain="base", portfolio=portfolio)

        balance = snapshot.balance(f"base:{self.CB_ADDR}")
        assert balance.symbol == f"base:{self.CB_ADDR}"
        assert balance.balance == Decimal("0")
        assert balance.balance_usd == Decimal("0")

    def test_snapshot_keeps_symbol_portfolio_token_distinct_from_address_key(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        state = self._real_state()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        portfolio.tokens["CBBTC"] = Decimal("0.05")
        snapshot = create_market_snapshot_from_state(state, chain="base", portfolio=portfolio)

        assert snapshot.price(self.CB_ADDR) == Decimal("60000")
        assert snapshot.balance("CBBTC").balance == Decimal("0.05")
        assert snapshot.balance(self.CB_ADDR).balance == Decimal("0")

    def test_token_aliases_argument_is_ignored_for_compatibility(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        state = self._real_state()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        portfolio.tokens["CBBTC"] = Decimal("0.05")
        snapshot = create_market_snapshot_from_state(
            state,
            chain="base",
            portfolio=portfolio,
            token_aliases={self.CB_ADDR: "CBBTC"},
        )

        assert snapshot.price(self.CB_ADDR) == Decimal("60000")
        assert snapshot.balance("CBBTC").balance == Decimal("0.05")
        assert snapshot.balance(self.CB_ADDR).balance == Decimal("0")

    def test_snapshot_balance_exposes_address_keyed_portfolio_token(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        state = self._real_state()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="base")
        portfolio.tokens[self.CB_KEY] = Decimal("0.05")
        snapshot = create_market_snapshot_from_state(state, chain="base", portfolio=portfolio)

        assert snapshot.balance(self.CB_ADDR).balance == Decimal("0.05")
        assert snapshot.balance(f"base:{self.CB_ADDR}").balance == Decimal("0.05")

    def test_snapshot_indicator_resolves_by_address(self):
        from almanak.framework.market import RSIData

        state = self._real_state()
        snapshot = create_market_snapshot_from_state(state, chain="base")
        snapshot.set_rsi(f"base:{self.CB_ADDR}", RSIData(value=Decimal("31"), period=14))
        assert snapshot.rsi(self.CB_ADDR, period=14).value == Decimal("31")


class TestAddressKeyedMarketState:
    """MarketState accepts address-native provider keys while preserving the symbol shim."""

    WSTETH_ADDR = "0x5979d7b546e38e414f7e9822514be443a4800529"
    WSTETH_KEY = ("arbitrum", WSTETH_ADDR)

    def _state(self):
        from almanak.framework.backtesting.pnl.data_provider import OHLCV, MarketState

        candle = OHLCV(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            open=Decimal("3400"),
            high=Decimal("3410"),
            low=Decimal("3390"),
            close=Decimal("3405"),
        )
        return MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="arbitrum",
            prices={self.WSTETH_KEY: Decimal("3400")},
            ohlcv={self.WSTETH_KEY: candle},
        )

    def test_get_price_reads_address_tuple_key(self):
        state = self._state()
        assert state.get_price(self.WSTETH_KEY) == Decimal("3400")
        assert state.get_price(self.WSTETH_ADDR.upper()) == Decimal("3400")

    def test_get_price_keeps_symbol_as_honest_miss_for_address_key(self):
        state = self._state()
        with pytest.raises(KeyError):
            state.get_price("WSTETH")

    def test_cross_chain_key_does_not_fall_back_to_current_chain_alias(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        state = MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={("base", self.WSTETH_ADDR): Decimal("3400")},
        )

        assert state.get_price(self.WSTETH_ADDR) == Decimal("3400")
        with pytest.raises(KeyError):
            state.get_price(("arbitrum", self.WSTETH_ADDR))
        with pytest.raises(KeyError):
            state.get_price(f"arbitrum:{self.WSTETH_ADDR}")

    def test_available_tokens_displays_addresses_for_address_keys(self):
        state = self._state()
        display_key = f"arbitrum:{self.WSTETH_ADDR}"
        assert state.available_tokens == [display_key]
        assert state.to_dict()["prices"] == {display_key: "3400"}
        assert state.get_price(display_key) == Decimal("3400")
        assert state.get_ohlcv(display_key).close == Decimal("3405")
        assert state.has_token(display_key) is True


class TestAddressKeyedSnapshotResolutionAllReads:
    """Address cache keys must be wired through every token-keyed snapshot read."""

    CB_ADDR = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
    CB_KEY = ("base", CB_ADDR)

    def _snapshot(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        state = MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={self.CB_KEY: Decimal("60000")},
        )
        return create_market_snapshot_from_state(state, chain="base")

    def test_macd_resolves_by_address(self):
        from almanak.framework.market import MACDData

        snap = self._snapshot()
        snap.set_macd(
            self.CB_ADDR, MACDData(macd_line=Decimal("1"), signal_line=Decimal("0.5"), histogram=Decimal("0.5"))
        )
        assert snap.macd(self.CB_ADDR).macd_line == Decimal("1")

    def test_atr_resolves_by_address(self):
        from almanak.framework.market import ATRData

        snap = self._snapshot()
        snap.set_atr(f"base:{self.CB_ADDR}", ATRData(value=Decimal("123"), value_percent=Decimal("0.2"), period=14))
        assert snap.atr(self.CB_ADDR, period=14).value == Decimal("123")

    def test_price_data_resolves_by_address(self):
        snap = self._snapshot()
        assert snap.price_data(self.CB_ADDR).price == Decimal("60000")


class TestSymbolAliasBridge:
    """Engine-registered token addresses bridge plain-symbol strategy reads.

    The bridge is fallback-only and explicit: only symbols in the run's own
    registered ``{SYMBOL: (chain, address)}`` map resolve onto address-native
    keys; everything else keeps the VIB-5508 honest-miss semantics pinned by
    the classes above (which construct snapshots WITHOUT a map).
    """

    WETH_ADDR = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_ADDR = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH_KEY = ("ethereum", WETH_ADDR)
    USDC_KEY = ("ethereum", USDC_ADDR)
    TOKEN_ADDRESSES = {"WETH": WETH_KEY, "USDC": USDC_KEY}

    def _state(self, prices=None):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        return MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="ethereum",
            prices=prices or {self.WETH_KEY: Decimal("3000"), self.USDC_KEY: Decimal("1")},
        )

    def _portfolio(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        return SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="ethereum")

    def test_price_resolves_registered_symbol(self):
        snapshot = create_market_snapshot_from_state(
            self._state(), chain="ethereum", token_addresses=self.TOKEN_ADDRESSES
        )
        assert snapshot.price("WETH") == Decimal("3000")
        assert snapshot.price("weth") == Decimal("3000")
        assert snapshot.price(self.WETH_ADDR) == Decimal("3000")

    def test_unregistered_symbol_stays_honest_miss(self):
        snapshot = create_market_snapshot_from_state(
            self._state(), chain="ethereum", token_addresses=self.TOKEN_ADDRESSES
        )
        with pytest.raises(ValueError, match="Cannot determine price"):
            snapshot.price("CBBTC")

    def test_balance_resolves_registered_symbol_for_holding(self):
        portfolio = self._portfolio()
        portfolio.tokens[self.WETH_KEY] = Decimal("2.5")
        snapshot = create_market_snapshot_from_state(
            self._state(), chain="ethereum", portfolio=portfolio, token_addresses=self.TOKEN_ADDRESSES
        )
        assert snapshot.balance("WETH").balance == Decimal("2.5")
        assert snapshot.balance(self.WETH_ADDR).balance == Decimal("2.5")

    def test_cash_stays_readable_through_alias(self):
        portfolio = self._portfolio()
        snapshot = create_market_snapshot_from_state(
            self._state(), chain="ethereum", portfolio=portfolio, token_addresses=self.TOKEN_ADDRESSES
        )
        assert snapshot.balance("USDC").balance == Decimal("10000")
        assert snapshot.balance("WETH").balance == Decimal("0")

    def test_cash_face_value_does_not_clobber_held_stable(self):
        portfolio = self._portfolio()
        portfolio.tokens[self.USDC_KEY] = Decimal("500")
        snapshot = create_market_snapshot_from_state(
            self._state(), chain="ethereum", portfolio=portfolio, token_addresses=self.TOKEN_ADDRESSES
        )
        assert snapshot.balance("USDC").balance == Decimal("500")
        assert snapshot.balance(self.USDC_ADDR).balance == Decimal("500")

    def test_mixed_case_map_keys_do_not_clobber_held_stable(self):
        # A direct caller may pass non-upper map keys; alias registration
        # upper-cases the alias name, so the held-key guard must probe the
        # same casing or the cash face value clobbers the tracked balance
        # (PR #3156 review).
        portfolio = self._portfolio()
        portfolio.tokens[self.USDC_KEY] = Decimal("500")
        snapshot = create_market_snapshot_from_state(
            self._state(),
            chain="ethereum",
            portfolio=portfolio,
            token_addresses={"weth": self.WETH_KEY, "usdc": self.USDC_KEY},
        )
        assert snapshot.balance("USDC").balance == Decimal("500")
        assert snapshot.balance(self.USDC_ADDR).balance == Decimal("500")
        assert snapshot.price("WETH") == Decimal("3000")

    def test_zero_seed_does_not_hide_cash_for_noncanonical_stable_address(self):
        # A funding entry may register a stable under a non-registry address
        # (e.g. a bridged variant). The zero-seed for that unheld address key
        # must not shadow the cash face value written through the alias.
        bridged_usdc = ("ethereum", "0x9999999999999999999999999999999999999999")
        portfolio = self._portfolio()
        snapshot = create_market_snapshot_from_state(
            self._state(prices={self.WETH_KEY: Decimal("3000"), bridged_usdc: Decimal("1")}),
            chain="ethereum",
            portfolio=portfolio,
            token_addresses={"WETH": self.WETH_KEY, "USDC": bridged_usdc},
        )
        assert snapshot.balance("USDC").balance == Decimal("10000")
        assert snapshot.balance(bridged_usdc[1]).balance == Decimal("10000")

    def test_indicator_set_by_address_reads_by_symbol(self):
        from almanak.framework.market import RSIData

        snapshot = create_market_snapshot_from_state(
            self._state(), chain="ethereum", token_addresses=self.TOKEN_ADDRESSES
        )
        snapshot.set_rsi(f"ethereum:{self.WETH_ADDR}", RSIData(value=Decimal("31"), period=14))
        assert snapshot.rsi("WETH", period=14).value == Decimal("31")
        assert snapshot.rsi(self.WETH_ADDR, period=14).value == Decimal("31")

    def test_cross_chain_entry_is_not_aliased(self):
        wsteth_key = ("arbitrum", "0x5979d7b546e38e414f7e9822514be443a4800529")
        snapshot = create_market_snapshot_from_state(
            self._state(), chain="ethereum", token_addresses={"WSTETH": wsteth_key}
        )
        with pytest.raises(ValueError, match="Cannot determine price"):
            snapshot.price("WSTETH")

    def test_malformed_entries_are_skipped(self):
        snapshot = create_market_snapshot_from_state(
            self._state(),
            chain="ethereum",
            token_addresses={
                self.WETH_ADDR: self.WETH_KEY,  # address-shaped name: not a symbol
                "WETH": ("ethereum", "not-an-address"),  # EVM-family target must be a 0x key
            },
        )
        with pytest.raises(ValueError, match="Cannot determine price"):
            snapshot.price("WETH")

    SOL_MINT = "So11111111111111111111111111111111111111112"
    SOL_KEY = ("solana", SOL_MINT)

    def test_solana_mint_alias_resolves_symbol_read(self):
        # Non-EVM chains key state by (chain, mint) with case-sensitive
        # base58 addresses; the alias bridge must cover them too or Solana
        # strategies lose the strategy-facing symbol surface (PR #3156
        # review). Family-aware target validation: opaque address accepted,
        # casing preserved.
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        state = MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="solana",
            prices={self.SOL_KEY: Decimal("150")},
        )
        snapshot = create_market_snapshot_from_state(state, chain="solana", token_addresses={"SOL": self.SOL_KEY})
        assert snapshot.price("SOL") == Decimal("150")
        assert snapshot.price(f"solana:{self.SOL_MINT}") == Decimal("150")

    def test_unknown_chain_target_is_skipped(self):
        # An alias must point at a key the engine could have seeded; a chain
        # the ChainRegistry does not know cannot be a run chain. Registered
        # directly because the engine-side helper already drops cross-chain
        # entries before they reach the snapshot guard.
        snapshot = create_market_snapshot_from_state(self._state(), chain="ethereum")
        snapshot._register_symbol_alias_keys({"FOO": "notachain:abcdef"})
        assert snapshot._symbol_alias_keys == {}
        with pytest.raises(ValueError, match="Cannot determine price"):
            snapshot.price("FOO")

    def test_symbol_keyed_portfolio_token_stays_distinct_without_map_entry(self):
        portfolio = self._portfolio()
        portfolio.tokens["CBBTC"] = Decimal("0.05")
        snapshot = create_market_snapshot_from_state(
            self._state(), chain="ethereum", portfolio=portfolio, token_addresses=self.TOKEN_ADDRESSES
        )
        assert snapshot.balance("CBBTC").balance == Decimal("0.05")


class TestSeededPriceHonorsQuote:
    """Engine-seeded snapshot reads honor ``quote`` — live/backtest parity.

    Regression (2026-07-24): the seeded fast path returned the engine-seeded
    USD price regardless of ``quote``, so a PnL backtest served ~$1880 for
    ``price("WETH", "ETH")`` on Base while live (gateway oracle, which passes
    ``quote`` upstream) served ~1.0. A strategy guarding
    ``abs(price("WETH", "ETH") - 1) > 0.01`` held every tick in backtest
    (0 trades) while trading live.
    """

    WETH_ADDR = "0x4200000000000000000000000000000000000006"
    USDC_ADDR = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    WETH_KEY = ("base", WETH_ADDR)
    USDC_KEY = ("base", USDC_ADDR)
    TOKEN_ADDRESSES = {"WETH": WETH_KEY, "USDC": USDC_KEY}

    def _snapshot(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        state = MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={self.WETH_KEY: Decimal("1880"), self.USDC_KEY: Decimal("1")},
        )
        return create_market_snapshot_from_state(state, chain="base", token_addresses=self.TOKEN_ADDRESSES)

    def test_weth_eth_quote_is_a_ratio_not_usd(self):
        # ETH is not a registered/seeded token; the quote leg resolves through
        # the chain registry's native<->wrapped equivalence onto seeded WETH.
        snapshot = self._snapshot()
        assert snapshot.price("WETH", "ETH") == Decimal("1")

    def test_usd_quote_still_returns_seeded_usd(self):
        snapshot = self._snapshot()
        assert snapshot.price("WETH") == Decimal("1880")
        assert snapshot.price("WETH", "USD") == Decimal("1880")

    def test_registered_token_quote_is_a_cross_rate(self):
        snapshot = self._snapshot()
        assert snapshot.price("WETH", "USDC") == Decimal("1880")

    def test_unregistered_quote_stays_honest_miss(self):
        snapshot = self._snapshot()
        with pytest.raises(ValueError, match="Cannot determine price"):
            snapshot.price("WETH", "CBBTC")


class TestSymbolIntentFlows:
    """Symbol-carrying intents resolve through the engine's registered map.

    Without the map, a plain-symbol intent leg on an address-keyed market
    state missed its price and fell back to $1-per-unit sizing under an
    unvalued symbol key — silent value minting (a $31 USD swap booked as
    31 WETH). Registered symbols must size at the real price and land on
    the address-native key; unregistered symbols keep the legacy fallback.
    """

    WETH_ADDR = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_ADDR = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH_KEY = ("ethereum", WETH_ADDR)
    USDC_KEY = ("ethereum", USDC_ADDR)
    TOKEN_ADDRESSES = {"WETH": WETH_KEY, "USDC": USDC_KEY}

    def _state(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        return MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="ethereum",
            prices={self.WETH_KEY: Decimal("2000"), self.USDC_KEY: Decimal("1")},
        )

    def test_swap_flows_resolve_symbols_through_registered_map(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl._engine_helpers import calculate_token_flows

        intent = SimpleNamespace(from_token="WETH", to_token="USDC")
        tokens_in, tokens_out = calculate_token_flows(
            intent, IntentType.SWAP, Decimal("31"), Decimal("0"), Decimal("0"), self._state(), self.TOKEN_ADDRESSES
        )
        assert tokens_out == {self.WETH_KEY: Decimal("31") / Decimal("2000")}
        assert tokens_in == {self.USDC_KEY: Decimal("31")}

    def test_swap_flows_refuse_unpriced_unregistered_symbols(self):
        """ALM-2943: the legacy $1-per-unit fallback for unregistered symbols
        was silent value minting (a $31 USD swap booked as 31 CBBTC). The
        typed flow lane now refuses to size an unpriced non-cash leg."""
        from types import SimpleNamespace

        import pytest

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl._engine_helpers import calculate_token_flows
        from almanak.framework.market.errors import PriceUnavailableError

        intent = SimpleNamespace(from_token="CBBTC", to_token="USDC")
        with pytest.raises(PriceUnavailableError):
            calculate_token_flows(
                intent, IntentType.SWAP, Decimal("31"), Decimal("0"), Decimal("0"), self._state(), self.TOKEN_ADDRESSES
            )

    def test_supply_flows_resolve_symbol_through_registered_map(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl._engine_helpers import calculate_token_flows

        intent = SimpleNamespace(token="WETH")
        _tokens_in, tokens_out = calculate_token_flows(
            intent, IntentType.SUPPLY, Decimal("100"), Decimal("0"), Decimal("0"), self._state(), self.TOKEN_ADDRESSES
        )
        assert tokens_out == {self.WETH_KEY: Decimal("100") / Decimal("2000")}

    def test_lp_open_flows_resolve_pool_symbols_through_registered_map(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl._engine_helpers import calculate_token_flows

        intent = SimpleNamespace(pool="WETH/USDC")
        _tokens_in, tokens_out = calculate_token_flows(
            intent, IntentType.LP_OPEN, Decimal("100"), Decimal("0"), Decimal("0"), self._state(), self.TOKEN_ADDRESSES
        )
        assert tokens_out == {
            self.WETH_KEY: Decimal("50") / Decimal("2000"),
            self.USDC_KEY: Decimal("50"),
        }

    def test_lp_position_mark_to_market_uses_registered_market_state_aliases(self):
        """Symbol-keyed LP positions must still read address-native prices."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from almanak.framework.backtesting.pnl.data_provider import MarketState
        from almanak.framework.backtesting.pnl.engine import PnLBacktester
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        bt = PnLBacktester(
            data_provider=MagicMock(_token_addresses=None),
            fee_models={},
            slippage_models={},
            token_addresses=self.TOKEN_ADDRESSES,
        )
        intent = SimpleNamespace(
            amount_usd=Decimal("1000"),
            pool="WETH/USDC",
            fee_tier=Decimal("0.003"),
            range_lower=Decimal("1000"),
            range_upper=Decimal("4000"),
        )
        entry_state = self._state()

        def marked_value(weth_price: str) -> Decimal:
            state = MarketState(
                timestamp=datetime(2026, 3, 23, tzinfo=UTC),
                chain="ethereum",
                prices={self.WETH_KEY: Decimal(weth_price), self.USDC_KEY: Decimal("1")},
            )
            state.register_symbol_aliases(self.TOKEN_ADDRESSES)
            position = bt._lp_open_delta(
                intent,
                protocol="uniswap_v3",
                tokens=["WETH", "USDC"],
                executed_price=Decimal("2000"),
                timestamp=entry_state.timestamp,
                market_state=entry_state,
                strict_reproducibility=False,
            )
            portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), chain="ethereum")
            return portfolio._mark_lp_position(position, state, state.timestamp)

        assert marked_value("2500") != marked_value("5000")

    def test_intent_amount_usd_prices_symbol_through_registered_map(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import get_intent_amount_usd

        intent = SimpleNamespace(amount=Decimal("2"), token="WETH")
        amount_usd = get_intent_amount_usd(intent, self._state(), token_addresses=self.TOKEN_ADDRESSES)
        assert amount_usd == Decimal("4000")

    def test_executed_price_resolves_symbol_through_registered_map(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.intent_extraction import get_executed_price

        intent = SimpleNamespace(from_token="WETH", to_token="USDC", token="WETH")
        price = get_executed_price(
            intent, self._state(), Decimal("0.01"), IntentType.SWAP, token_addresses=self.TOKEN_ADDRESSES
        )
        assert price == Decimal("2000") * Decimal("0.99")

    def test_supply_position_delta_resolves_symbol_amount_and_key(self):
        """A symbol SUPPLY must book real units under the address key.

        Pre-fix the position booked amount_usd as token units under the
        symbol key while entry_price carried the real price — a $453 supply
        marked as 453 WETH (~$1M of minted equity in the spark demo).
        """
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from almanak.framework.backtesting.pnl.engine import PnLBacktester

        bt = PnLBacktester(
            data_provider=MagicMock(_token_addresses=None),
            fee_models={},
            slippage_models={},
            token_addresses=self.TOKEN_ADDRESSES,
        )
        intent = SimpleNamespace(amount_usd=Decimal("453"), apy=Decimal("0.05"))
        position = bt._supply_delta(
            intent,
            protocol="spark",
            tokens=["WETH"],
            executed_price=Decimal("2000"),
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            market_state=self._state(),
            strict_reproducibility=False,
        )
        assert self.WETH_KEY in position.tokens
        assert position.total_amount == Decimal("453") / Decimal("2000")

    def test_repay_matches_address_keyed_borrow_position_by_symbol(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import find_borrow_close_position_id
        from almanak.framework.backtesting.pnl.position_models import PositionType

        position = SimpleNamespace(
            position_type=PositionType.BORROW,
            tokens=[self.USDC_KEY],
            protocol="benqi",
            position_id="borrow-1",
            entry_time=datetime(2026, 3, 23, tzinfo=UTC),
        )
        intent = SimpleNamespace(token="USDC", protocol="benqi")
        matched = find_borrow_close_position_id(intent, [position], self.TOKEN_ADDRESSES)
        assert matched == "borrow-1"

    def test_repay_does_not_match_unregistered_symbol(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.pnl.intent_extraction import find_borrow_close_position_id
        from almanak.framework.backtesting.pnl.position_models import PositionType

        position = SimpleNamespace(
            position_type=PositionType.BORROW,
            tokens=[self.USDC_KEY],
            protocol="benqi",
            position_id="borrow-1",
            entry_time=datetime(2026, 3, 23, tzinfo=UTC),
        )
        intent = SimpleNamespace(token="CBBTC", protocol="benqi")
        assert find_borrow_close_position_id(intent, [position], self.TOKEN_ADDRESSES) is None


class TestAddressKeyedFlows:
    """Address-keyed intents preserve token identity in flows and positions."""

    CB_ADDR = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
    USDC_ADDR = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    CB_KEY = ("base", CB_ADDR)
    USDC_KEY = ("base", USDC_ADDR)

    def _state(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        return MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={self.CB_KEY: Decimal("60000"), self.USDC_KEY: Decimal("1")},
        )

    def test_swap_flows_keyed_by_address(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl._engine_helpers import calculate_token_flows

        intent = SimpleNamespace(from_token=self.USDC_ADDR, to_token=self.CB_ADDR)
        tokens_in, tokens_out = calculate_token_flows(
            intent, IntentType.SWAP, Decimal("60"), Decimal("0"), Decimal("0"), self._state()
        )
        assert set(tokens_out) == {self.USDC_KEY}
        assert set(tokens_in) == {self.CB_KEY}

    def test_supply_flow_keyed_by_address(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl._engine_helpers import calculate_token_flows

        intent = SimpleNamespace(token=self.CB_ADDR)
        _tokens_in, tokens_out = calculate_token_flows(
            intent, IntentType.SUPPLY, Decimal("60"), Decimal("0"), Decimal("0"), self._state()
        )
        assert set(tokens_out) == {self.CB_KEY}

    def test_vault_deposit_position_keyed_by_address(self):
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from almanak.framework.backtesting.pnl.engine import PnLBacktester

        bt = PnLBacktester(data_provider=MagicMock(), fee_models={}, slippage_models={})
        intent = SimpleNamespace(deposit_token=self.CB_ADDR, amount_usd=Decimal("60"), apy=Decimal("0.05"))
        position = bt._vault_deposit_delta(
            intent,
            protocol="metamorpho",
            tokens=["CBBTC"],
            executed_price=Decimal("60000"),
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            market_state=self._state(),
            strict_reproducibility=False,
        )
        assert self.CB_KEY in position.tokens
