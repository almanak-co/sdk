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
        state = _make_market_state({
            "WETH": Decimal("3000"),
            "WBTC": Decimal("60000"),
            "ARB": Decimal("1.5"),
        })
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


class TestAddressAliasResolution:
    """A strategy may reference tokens by contract address; the backtest data
    (seeded by symbol) must resolve those address reads via the alias map."""

    CB_ADDR = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
    ALIASES = {CB_ADDR: "CBBTC"}

    def _real_state(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        return MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={"CBBTC": Decimal("60000"), "USDC": Decimal("1")},
            token_aliases=self.ALIASES,
        )

    def test_marketstate_get_price_resolves_address_any_case(self):
        state = self._real_state()
        assert state.get_price(self.CB_ADDR) == Decimal("60000")
        assert state.get_price(self.CB_ADDR.upper()) == Decimal("60000")  # fill-pricing keys uppercase
        assert state.get_price("CBBTC") == Decimal("60000")  # symbol still works

    def test_marketstate_unmapped_address_is_honest_miss(self):
        state = self._real_state()
        with pytest.raises(KeyError):
            state.get_price("0xdeadbeef00000000000000000000000000000000")

    def test_snapshot_price_and_balance_resolve_by_address(self):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        state = self._real_state()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        portfolio.tokens["CBBTC"] = Decimal("0.05")
        snapshot = create_market_snapshot_from_state(
            state, chain="base", portfolio=portfolio, token_aliases=self.ALIASES
        )
        assert snapshot.price(self.CB_ADDR) == snapshot.price("CBBTC") == Decimal("60000")
        assert snapshot.balance(self.CB_ADDR).balance == Decimal("0.05")

    def test_snapshot_indicator_resolves_by_address(self):
        from almanak.framework.market import RSIData

        state = self._real_state()
        snapshot = create_market_snapshot_from_state(state, chain="base", token_aliases=self.ALIASES)
        snapshot.set_rsi("CBBTC", RSIData(value=Decimal("31"), period=14))
        assert snapshot.rsi(self.CB_ADDR, period=14).value == Decimal("31")

    def test_empty_aliases_keeps_address_an_honest_miss(self):
        # Without an alias map (live snapshots), an address query is unchanged.
        state = self._real_state()
        state.token_aliases = {}
        snapshot = create_market_snapshot_from_state(state, chain="base")
        with pytest.raises(ValueError):
            snapshot.price(self.CB_ADDR)


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
            token_aliases={self.WSTETH_ADDR: "WSTETH"},
        )

    def test_get_price_reads_address_tuple_key(self):
        state = self._state()
        assert state.get_price(self.WSTETH_KEY) == Decimal("3400")
        assert state.get_price(self.WSTETH_ADDR.upper()) == Decimal("3400")

    def test_get_price_resolves_symbol_to_address_key_during_transition(self):
        state = self._state()
        assert state.get_price("WSTETH") == Decimal("3400")

    def test_available_tokens_displays_addresses_for_address_keys(self):
        state = self._state()
        display_key = f"arbitrum:{self.WSTETH_ADDR}"
        assert state.available_tokens == [display_key]
        assert state.to_dict()["prices"] == {display_key: "3400"}


class TestAddressAliasResolutionAllReads:
    """`_canonicalize_token` must be wired through every token-keyed snapshot read,
    not just price/balance/rsi/bb/ema."""

    CB_ADDR = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
    ALIASES = {CB_ADDR: "CBBTC"}

    def _snapshot(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        state = MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={"CBBTC": Decimal("60000")},
            token_aliases=self.ALIASES,
        )
        return create_market_snapshot_from_state(state, chain="base", token_aliases=self.ALIASES)

    def test_macd_resolves_by_address(self):
        from almanak.framework.market import MACDData

        snap = self._snapshot()
        snap.set_macd("CBBTC", MACDData(macd_line=Decimal("1"), signal_line=Decimal("0.5"), histogram=Decimal("0.5")))
        assert snap.macd(self.CB_ADDR).macd_line == Decimal("1")

    def test_atr_resolves_by_address(self):
        from almanak.framework.market import ATRData

        snap = self._snapshot()
        snap.set_atr("CBBTC", ATRData(value=Decimal("123"), value_percent=Decimal("0.2"), period=14))
        assert snap.atr(self.CB_ADDR, period=14).value == Decimal("123")

    def test_price_data_resolves_by_address(self):
        snap = self._snapshot()
        assert snap.price_data(self.CB_ADDR).price == Decimal("60000")


class TestAliasAwareFlows:
    """Address-keyed intents must produce SYMBOL-keyed portfolio flows / position
    labels so the cash sweep, cost-basis, and close/reporting stay symbol-keyed."""

    CB_ADDR = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
    USDC_ADDR = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    ALIASES = {CB_ADDR: "CBBTC", USDC_ADDR: "USDC"}

    def _state(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        return MarketState(
            timestamp=datetime(2026, 3, 23, tzinfo=UTC),
            chain="base",
            prices={"CBBTC": Decimal("60000"), "USDC": Decimal("1")},
            token_aliases=self.ALIASES,
        )

    def test_swap_flows_keyed_by_symbol(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl._engine_helpers import calculate_token_flows

        intent = SimpleNamespace(from_token=self.USDC_ADDR, to_token=self.CB_ADDR)
        tokens_in, tokens_out = calculate_token_flows(
            intent, IntentType.SWAP, Decimal("60"), Decimal("0"), Decimal("0"), self._state()
        )
        assert set(tokens_out) == {"USDC"}  # not "0X8335..."
        assert set(tokens_in) == {"CBBTC"}  # not "0XCBB7..."

    def test_supply_flow_keyed_by_symbol(self):
        from types import SimpleNamespace

        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl._engine_helpers import calculate_token_flows

        intent = SimpleNamespace(token=self.CB_ADDR)
        _tokens_in, tokens_out = calculate_token_flows(
            intent, IntentType.SUPPLY, Decimal("60"), Decimal("0"), Decimal("0"), self._state()
        )
        assert set(tokens_out) == {"CBBTC"}

    def test_vault_deposit_position_labelled_by_symbol(self):
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
        assert "CBBTC" in position.tokens  # not "0XCBB7..."
