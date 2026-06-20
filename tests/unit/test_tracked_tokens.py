"""Tests for _get_tracked_tokens and _derive_tokens_from_config."""

from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak import IntentStrategy
from almanak.framework.portfolio.models import ValueConfidence


# ---------------------------------------------------------------------------
# Minimal config fixtures
# ---------------------------------------------------------------------------

@dataclass
class PoolConfig:
    """Config with pool field (LP strategies)."""
    pool: str = "WETH/USDC/500"
    range_width_pct: Decimal = Decimal("0.20")
    amount0: Decimal = Decimal("0.001")
    amount1: Decimal = Decimal("3")
    deployment_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


@dataclass
class SwapConfig:
    """Config with base_token/quote_token fields (swap strategies)."""
    base_token: str = "WETH"
    quote_token: str = "USDC"
    trade_amount: str = "100"
    deployment_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {
            "base_token": self.base_token,
            "quote_token": self.quote_token,
            "trade_amount": self.trade_amount,
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


@dataclass
class LendingConfig:
    """Config with collateral_token/borrow_token fields (lending strategies)."""
    collateral_token: str = "wstETH"
    borrow_token: str = "USDC"
    deployment_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {
            "collateral_token": self.collateral_token,
            "borrow_token": self.borrow_token,
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


@dataclass
class FromToConfig:
    """Config with from_token/to_token fields (simple swap strategies)."""
    from_token: str = "WETH"
    to_token: str = "USDC"
    deployment_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {
            "from_token": self.from_token,
            "to_token": self.to_token,
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


@dataclass
class EmptyConfig:
    """Config with no token-related fields."""
    interval: int = 60
    deployment_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {"interval": self.interval}

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


# ---------------------------------------------------------------------------
# Helper to create a strategy instance without the full framework
# ---------------------------------------------------------------------------

class _ConcreteStrategy(IntentStrategy):
    """Minimal concrete subclass for testing."""

    def decide(self, market):
        return None

    def get_open_positions(self):
        from almanak.framework.teardown.models import TeardownPositionSummary
        return TeardownPositionSummary.empty(getattr(self, "_deployment_id", "test"))

    def generate_teardown_intents(self, mode=None, market=None):
        return []


def _make_strategy(config):
    """Create a strategy instance with minimal mocking."""
    strategy = object.__new__(_ConcreteStrategy)
    strategy.config = config
    strategy._chain = getattr(config, "chain", "arbitrum")
    strategy._deployment_id = getattr(config, "deployment_id", "test")
    return strategy


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDeriveTokensFromConfig:
    """Test _derive_tokens_from_config extracts tokens correctly."""

    def test_pool_field_extracts_tokens(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/500"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC"]

    def test_pool_field_two_token_format(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC"]

    def test_pool_field_skips_fee_tier(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/3000"))
        tokens = strategy._derive_tokens_from_config()
        assert "3000" not in tokens
        assert tokens == ["WETH", "USDC"]

    def test_base_quote_tokens(self):
        strategy = _make_strategy(SwapConfig(base_token="WETH", quote_token="USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert len(tokens) == 2

    def test_collateral_borrow_tokens(self):
        strategy = _make_strategy(LendingConfig(collateral_token="wstETH", borrow_token="USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert "wstETH" in tokens
        assert "USDC" in tokens
        assert len(tokens) == 2

    def test_from_to_tokens(self):
        strategy = _make_strategy(FromToConfig(from_token="WETH", to_token="USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert len(tokens) == 2

    def test_empty_config_returns_empty(self):
        strategy = _make_strategy(EmptyConfig())
        tokens = strategy._derive_tokens_from_config()
        assert tokens == []

    def test_no_duplicates(self):
        """If same token appears in multiple fields, it should appear once."""
        @dataclass
        class DupConfig:
            base_token: str = "USDC"
            quote_token: str = "USDC"
            deployment_id: str = "test"
            chain: str = "arbitrum"
            def to_dict(self):
                return {"base_token": self.base_token, "quote_token": self.quote_token}
            def update(self, **kwargs):
                pass

        strategy = _make_strategy(DupConfig())
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["USDC"]

    def test_pool_with_bridged_tokens(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC.e/500"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC.e"]

    def test_traderjoe_pool_format(self):
        strategy = _make_strategy(PoolConfig(pool="WAVAX/USDC/20"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WAVAX", "USDC"]

    def test_pool_field_skips_volatile_suffix(self):
        """Regression: Aerodrome 'volatile' suffix must not leak as a token.

        Observed in staging deployment b3f41304: the pool 'WETH/USDC/volatile'
        was producing 'No Chainlink feed for VOLATILE on base' errors because
        _derive_tokens_from_config returned ['WETH', 'USDC', 'volatile'] and
        the portfolio valuer/pre-warmer then queried price/balance for
        'volatile'.
        """
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/volatile"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC"]
        assert "volatile" not in tokens

    def test_pool_field_skips_stable_suffix(self):
        strategy = _make_strategy(PoolConfig(pool="USDC/USDT/stable"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["USDC", "USDT"]
        assert "stable" not in tokens

    def test_pool_field_skips_concentrated_suffix(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/concentrated"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC"]
        assert "concentrated" not in tokens

    def test_pool_field_skips_cl_suffix(self):
        """Aerodrome Slipstream 'cl' suffix."""
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/cl"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC"]
        assert "cl" not in tokens

    def test_market_id_not_treated_as_token(self):
        """Compound V3 market IDs like 'usdc_e' must NOT leak as token symbols.

        Regression test for VIB-2675: market field without "/" is a market ID,
        not a pool descriptor. Passing it to the token resolver causes 90s
        timeout loops on Polygon.
        """
        @dataclass
        class CompoundV3Config:
            market: str = "usdc_e"
            collateral_token: str = "WETH"
            borrow_token: str = "USDC"
            deployment_id: str = "test"
            chain: str = "polygon"
            def to_dict(self):
                return {
                    "market": self.market,
                    "collateral_token": self.collateral_token,
                    "borrow_token": self.borrow_token,
                }
            def update(self, **kwargs):
                pass

        strategy = _make_strategy(CompoundV3Config())
        tokens = strategy._derive_tokens_from_config()
        assert "usdc_e" not in tokens
        assert "WETH" in tokens
        assert "USDC" in tokens

    def test_market_id_usdc_not_treated_as_token(self):
        """Single-word market IDs like 'usdc' are also excluded."""
        @dataclass
        class CompoundV3SimpleConfig:
            market: str = "usdc"
            base_token: str = "USDC"
            deployment_id: str = "test"
            chain: str = "arbitrum"
            def to_dict(self):
                return {"market": self.market, "base_token": self.base_token}
            def update(self, **kwargs):
                pass

        strategy = _make_strategy(CompoundV3SimpleConfig())
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["USDC"]

    def test_market_field_with_slash_still_parsed(self):
        """Market field with '/' format should still be parsed as a pool."""
        @dataclass
        class MarketPoolConfig:
            market: str = "WETH/USDC"
            deployment_id: str = "test"
            chain: str = "arbitrum"
            def to_dict(self):
                return {"market": self.market}
            def update(self, **kwargs):
                pass

        strategy = _make_strategy(MarketPoolConfig())
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC"]

    def test_fiat_quote_in_market_descriptor_excluded(self):
        """Regression: market='BTC/USD' must not leak USD as a tracked token.

        Observed on BSC staging (2026-04-22): strategy with market='BTC/USD'
        caused the tracked-tokens loop to call balance("USD") and price("USD"),
        both of which fail — USD is a quote denomination, not an ERC20, and
        Chainlink has no USD/USD feed.
        """
        @dataclass
        class PerpMarketConfig:
            market: str = "BTC/USD"
            deployment_id: str = "test"
            chain: str = "bsc"
            def to_dict(self):
                return {"market": self.market}
            def update(self, **kwargs):
                pass

        strategy = _make_strategy(PerpMarketConfig())
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["BTC"]
        assert "USD" not in tokens

    def test_fiat_quote_token_field_excluded(self):
        """quote_token='USD' must not leak as a tracked token."""
        strategy = _make_strategy(SwapConfig(base_token="WETH", quote_token="USD"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH"]
        assert "USD" not in tokens

    def test_stablecoin_quote_token_still_tracked(self):
        """USDC/USDT/DAI are real tokens and must NOT be filtered as fiat."""
        strategy = _make_strategy(SwapConfig(base_token="WETH", quote_token="USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert "USDC" in tokens
        assert "WETH" in tokens

    def test_fiat_eur_gbp_jpy_excluded(self):
        """EUR/GBP/JPY are also pure fiat and have no on-chain representation."""
        for fiat in ("EUR", "GBP", "JPY"):
            strategy = _make_strategy(SwapConfig(base_token="WETH", quote_token=fiat))
            tokens = strategy._derive_tokens_from_config()
            assert tokens == ["WETH"], f"{fiat} should be filtered"


class TestGetTrackedTokens:
    """Test _get_tracked_tokens returns derived tokens or fallback."""

    def test_returns_derived_tokens_when_available(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/500"))
        tokens = strategy._get_tracked_tokens()
        assert tokens == ["WETH", "USDC"]

    def test_fallback_when_no_tokens_in_config(self):
        strategy = _make_strategy(EmptyConfig())
        tokens = strategy._get_tracked_tokens()
        assert tokens == ["USDC", "WETH"]

    def test_does_not_include_unrelated_tokens(self):
        """The key bug fix: LP strategy should NOT fetch USDT, DAI, ETH."""
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/500"))
        tokens = strategy._get_tracked_tokens()
        assert "USDT" not in tokens
        assert "DAI" not in tokens
        assert "ETH" not in tokens

    def test_config_with_none_value(self):
        """Config fields with None values should be skipped."""
        @dataclass
        class NullConfig:
            base_token: str = "WETH"
            quote_token: str | None = None
            deployment_id: str = "test"
            chain: str = "arbitrum"
            def to_dict(self):
                return {"base_token": self.base_token, "quote_token": self.quote_token}
            def update(self, **kwargs):
                pass

        strategy = _make_strategy(NullConfig())
        tokens = strategy._get_tracked_tokens()
        assert tokens == ["WETH"]


# ---------------------------------------------------------------------------
# VIB-3937 — get_portfolio_snapshot must include the chain's NATIVE
# gas-token (ETH/MATIC/AVAX/...) in wallet_balances. Without it the wallet-
# method PnL silently misses gas spend (G6 reconciliation gap on every run
# equals exactly Σ_gas_usd).
# ---------------------------------------------------------------------------


class _StubMarket:
    """Minimal MarketSnapshot stub that returns balances/prices from dicts.

    Mirrors the surface area get_portfolio_snapshot uses: ``balance(token)``
    returns an object with a ``.balance`` Decimal attribute, ``price(token)``
    returns a Decimal. Missing keys raise KeyError so the production
    try/except in get_portfolio_snapshot exercises the same ``continue``
    path it takes in the wild.
    """

    def __init__(self, balances: dict, prices: dict) -> None:
        self._balances = balances
        self._prices = prices

    def balance(self, token, protocol=None, *, chain=None, price=None):
        from types import SimpleNamespace

        return SimpleNamespace(balance=self._balances[token])

    def price(self, token, quote="USD", *, chain=None):
        return self._prices[token]


class TestVib3937NativeTokenInWalletSnapshot:
    """get_portfolio_snapshot must append the chain's native gas-token to
    wallet_balances after the tracked-tokens loop. Native is gated only by
    a successful balance/price read (NOT by `> 0`) — for any successful
    run on a chain that consumed gas, native must have been > 0."""

    def _make_arbitrum_strategy(self) -> _ConcreteStrategy:
        config = PoolConfig(pool="WETH/USDC/500", chain="arbitrum")
        return _make_strategy(config)

    def test_native_eth_appears_in_wallet_balances_on_arbitrum(self) -> None:
        strategy = self._make_arbitrum_strategy()
        # Tracked tokens on this fixture are WETH + USDC. Native is ETH.
        market = _StubMarket(
            balances={
                "WETH": Decimal("0.5"),
                "USDC": Decimal("100"),
                "ETH": Decimal("0.987654321"),  # the native gas-token
            },
            prices={
                "WETH": Decimal("2300"),
                "USDC": Decimal("1"),
                "ETH": Decimal("2300"),
            },
        )

        snap = strategy.get_portfolio_snapshot(market=market)

        symbols = {b.symbol for b in snap.wallet_balances}
        assert "ETH" in symbols, (
            f"VIB-3937: native ETH must appear in wallet_balances; got {sorted(symbols)}"
        )
        eth_row = next(b for b in snap.wallet_balances if b.symbol == "ETH")
        assert eth_row.balance == Decimal("0.987654321")
        assert eth_row.price_usd == Decimal("2300")
        assert eth_row.value_usd == Decimal("0.987654321") * Decimal("2300")
        # Sanity: tracked-token rows still there.
        assert {"WETH", "USDC"}.issubset(symbols)
        # available_cash_usd includes the native bucket.
        expected_cash = (
            Decimal("0.5") * Decimal("2300")  # WETH
            + Decimal("100") * Decimal("1")  # USDC
            + Decimal("0.987654321") * Decimal("2300")  # native ETH (NEW)
        )
        assert snap.available_cash_usd == expected_cash

    def test_native_avax_on_avalanche(self) -> None:
        config = PoolConfig(pool="WAVAX/USDC/500", chain="avalanche")
        strategy = _make_strategy(config)
        market = _StubMarket(
            balances={
                "WAVAX": Decimal("0"),
                "USDC": Decimal("0"),
                "AVAX": Decimal("1.5"),  # native
            },
            prices={
                "WAVAX": Decimal("30"),
                "USDC": Decimal("1"),
                "AVAX": Decimal("30"),
            },
        )

        snap = strategy.get_portfolio_snapshot(market=market)

        symbols = {b.symbol for b in snap.wallet_balances}
        assert "AVAX" in symbols, (
            f"VIB-3937: chain=avalanche native must be AVAX; got {sorted(symbols)}"
        )
        # WAVAX/USDC are zero so they're skipped by the `balance > 0` gate above.
        # Native is included even if it were 0 (it's non-zero here anyway).
        assert "WAVAX" not in symbols
        assert "USDC" not in symbols

    def test_native_fetch_failure_does_not_blank_snapshot(self) -> None:
        """Fail-open contract: if the native balance/price is unfetchable,
        the snapshot still returns with the tracked tokens it could read
        (and value_confidence stays HIGH per the existing positions path)."""
        strategy = self._make_arbitrum_strategy()
        # ETH is deliberately absent → KeyError on market.balance("ETH")
        market = _StubMarket(
            balances={"WETH": Decimal("0.5"), "USDC": Decimal("100")},
            prices={"WETH": Decimal("2300"), "USDC": Decimal("1")},
        )

        snap = strategy.get_portfolio_snapshot(market=market)

        symbols = {b.symbol for b in snap.wallet_balances}
        # Tracked tokens still captured…
        assert {"WETH", "USDC"}.issubset(symbols)
        # …native silently absent (the production try/except swallowed it).
        assert "ETH" not in symbols

    def test_native_not_duplicated_when_already_in_tracked_tokens(self) -> None:
        """Defensive: a strategy whose config explicitly tracks "ETH" (rare,
        but possible — strategies that hold native stable as an asset)
        should not get a duplicate row from the native pass."""

        @dataclass
        class _NativeConfig:
            pool: str = "ETH/USDC/500"
            range_width_pct: Decimal = Decimal("0.20")
            amount0: Decimal = Decimal("0.001")
            amount1: Decimal = Decimal("3")
            deployment_id: str = "test"
            strategy_name: str = "test"
            chain: str = "arbitrum"

            def to_dict(self):
                return {"pool": self.pool}

            def update(self, **kwargs):
                for k, v in kwargs.items():
                    if hasattr(self, k):
                        setattr(self, k, v)

        strategy = _make_strategy(_NativeConfig())
        market = _StubMarket(
            balances={"ETH": Decimal("0.5"), "USDC": Decimal("100")},
            prices={"ETH": Decimal("2300"), "USDC": Decimal("1")},
        )

        snap = strategy.get_portfolio_snapshot(market=market)

        eth_rows = [b for b in snap.wallet_balances if b.symbol == "ETH"]
        assert len(eth_rows) == 1, (
            f"VIB-3937: ETH already in tracked tokens must not be duplicated; got {eth_rows}"
        )


class TestVib5271FallbackTotalIsPositionsOnly:
    """VIB-5271: the strategy fallback snapshot's ``total_value_usd`` must be
    strategy-scoped (open-position value ONLY), per VIB-3614 and matching the
    canonical ``PortfolioValuer``. Wallet cash lives in ``available_cash_usd``.

    The pre-fix bug set ``total_value_usd = position_value + wallet_value`` while
    ``available_cash_usd = wallet_value``, so the NAV consumer formula
    ``total_value_usd + available_cash_usd`` (quant_aggregations.py NAV/drawdown
    fold, portfolio/models.py) double-counted the wallet. No prior test pinned
    the fallback arithmetic — which is why that regression slipped (it only
    surfaced via the VIB-5252 perp self-review). This class is that pin.
    """

    @staticmethod
    def _strategy_reporting_position(value_usd: Decimal) -> _ConcreteStrategy:
        """An arbitrum strategy whose get_open_positions reports ONE real
        protocol position worth ``value_usd`` (so position_value is non-zero
        and the wallet double-count is observable)."""
        from datetime import UTC, datetime

        from almanak.framework.teardown.models import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        config = PoolConfig(pool="WETH/USDC/500", chain="arbitrum")
        strategy = _make_strategy(config)

        def _get_open_positions() -> TeardownPositionSummary:
            return TeardownPositionSummary(
                deployment_id="test",
                timestamp=datetime.now(UTC),
                positions=[
                    PositionInfo(
                        position_type=PositionType.LP,
                        position_id="lp-WETH/USDC-arbitrum",
                        chain="arbitrum",
                        protocol="uniswap_v3",
                        value_usd=value_usd,
                    )
                ],
                total_value_usd=value_usd,
            )

        strategy.get_open_positions = _get_open_positions  # type: ignore[method-assign]
        return strategy

    def test_fallback_total_value_is_positions_only_no_wallet_double_count(self) -> None:
        position_value = Decimal("2000")
        strategy = self._strategy_reporting_position(position_value)
        market = _StubMarket(
            balances={
                "WETH": Decimal("0.5"),
                "USDC": Decimal("100"),
                "ETH": Decimal("0.987654321"),  # native gas-token
            },
            prices={
                "WETH": Decimal("2300"),
                "USDC": Decimal("1"),
                "ETH": Decimal("2300"),
            },
        )
        expected_cash = (
            Decimal("0.5") * Decimal("2300")  # WETH
            + Decimal("100") * Decimal("1")  # USDC
            + Decimal("0.987654321") * Decimal("2300")  # native ETH
        )

        snap = strategy.get_portfolio_snapshot(market=market)

        # total_value_usd is positions-only (VIB-3614) — wallet NOT folded in.
        assert snap.total_value_usd == position_value
        # Wallet cash carried separately, unchanged.
        assert snap.available_cash_usd == expected_cash
        # NAV reconstruction counts the wallet EXACTLY ONCE.
        nav = snap.total_value_usd + snap.available_cash_usd
        assert nav == position_value + expected_cash
        # And specifically NOT the pre-fix double-count.
        assert nav != position_value + 2 * expected_cash
        # Positions were available → HIGH confidence.
        assert snap.value_confidence == ValueConfidence.HIGH

    def test_fallback_total_matches_open_position_value(self) -> None:
        """The fallback's total_value_usd must equal the reported open-position
        value (the same quantity the canonical PortfolioValuer emits), with zero
        wallet contribution."""
        position_value = Decimal("7531.42")
        strategy = self._strategy_reporting_position(position_value)
        market = _StubMarket(
            balances={"WETH": Decimal("1"), "USDC": Decimal("0"), "ETH": Decimal("0.1")},
            prices={"WETH": Decimal("2300"), "USDC": Decimal("1"), "ETH": Decimal("2300")},
        )

        snap = strategy.get_portfolio_snapshot(market=market)

        assert snap.total_value_usd == position_value
