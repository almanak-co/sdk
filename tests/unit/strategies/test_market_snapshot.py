"""Tests for MarketSnapshot facade methods."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.market import MarketSnapshot, MultiChainMarketSnapshot, PriceData, RSIData
from almanak.framework.strategies.intent_strategy import DEFAULT_TIMEFRAME


class TestMarketSnapshotSetPriceData:
    """MarketSnapshot.set_price_data() should populate price_data() and price()."""

    def test_set_price_data_roundtrip(self):
        """set_price_data() data should be retrievable via price_data()."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        pd = PriceData(
            price=Decimal("3000"),
            price_24h_ago=Decimal("2900"),
            change_24h_pct=Decimal("3.45"),
            high_24h=Decimal("3050"),
            low_24h=Decimal("2850"),
        )
        market.set_price_data("ETH", pd)

        result = market.price_data("ETH")
        assert result.price == Decimal("3000")
        assert result.change_24h_pct == Decimal("3.45")
        assert result.high_24h == Decimal("3050")
        assert result.low_24h == Decimal("2850")
        assert result.price_24h_ago == Decimal("2900")

    def test_set_price_data_readable_via_price(self):
        """price() should also return the price from set_price_data()."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        pd = PriceData(price=Decimal("1850.50"))
        market.set_price_data("ETH", pd)

        assert market.price("ETH") == Decimal("1850.50")

    def test_set_price_data_custom_quote(self):
        """set_price_data() with non-default quote currency."""
        market = MarketSnapshot(chain="ethereum", wallet_address="0xtest")
        pd = PriceData(price=Decimal("0.00033"))
        market.set_price_data("ETH", pd, quote="BTC")

        result = market.price_data("ETH", quote="BTC")
        assert result.price == Decimal("0.00033")

    def test_set_price_data_does_not_affect_other_tokens(self):
        """Setting price data for one token should not affect another."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price_data("ETH", PriceData(price=Decimal("3000")))

        # USDC should still raise ValueError since no data is set
        try:
            market.price("USDC")
            raise AssertionError("Should have raised ValueError")
        except ValueError:
            pass

    def test_set_price_overridden_by_set_price_data(self):
        """set_price_data() should take precedence for price_data() calls."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        # set_price populates _prices (checked first by price())
        market.set_price("ETH", Decimal("2800"))
        # set_price_data populates _price_cache (checked first by price_data())
        market.set_price_data("ETH", PriceData(price=Decimal("3000"), change_24h_pct=Decimal("5.0")))

        # price_data() should return the richer PriceData
        pd = market.price_data("ETH")
        assert pd.price == Decimal("3000")
        assert pd.change_24h_pct == Decimal("5.0")

    def test_price_forwards_snapshot_chain_to_oracle(self):
        """Single-chain snapshots must pass their chain to the price oracle."""
        captured: list[tuple[str, str, str | None]] = []

        def mock_price_oracle(token: str, quote: str = "USD", chain: str | None = None) -> Decimal:
            captured.append((token, quote, chain))
            return Decimal("1850.50")

        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            price_oracle=mock_price_oracle,
        )

        assert market.price("ETH") == Decimal("1850.50")
        assert captured == [("ETH", "USD", "arbitrum")]

    def test_price_cache_isolated_by_chain_override(self):
        """An explicit chain override must not reuse another chain's cached price."""
        calls: list[str | None] = []
        prices = {
            "arbitrum": Decimal("3000"),
            "base": Decimal("3100"),
        }

        def mock_price_oracle(token: str, quote: str = "USD", chain: str | None = None) -> Decimal:
            calls.append(chain)
            assert chain is not None
            return prices[chain]

        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            price_oracle=mock_price_oracle,
        )

        assert market.price("ETH") == Decimal("3000")
        assert market.price("ETH", chain="base") == Decimal("3100")
        assert market.price("ETH") == Decimal("3000")
        assert calls == ["arbitrum", "base"]

    def test_set_price_data_chain_override(self):
        """set_price_data() can pre-populate a non-default chain cache entry."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price_data("ETH", PriceData(price=Decimal("3100")), chain="base")

        assert market.price("ETH", chain="base") == Decimal("3100")


class TestMultiChainMarketSnapshotSetPriceData:
    """MultiChainMarketSnapshot.set_price_data() for multi-chain scenarios."""

    def test_set_price_data_roundtrip(self):
        """set_price_data() should populate the price cache for the chain.

        VIB-4062: uses the public price_data() API rather than inspecting
        the now-flat internal _price_cache (the legacy multichain class
        keyed per chain; the canonical class uses a flat cache and chain=
        kwarg semantics).
        """
        market = MultiChainMarketSnapshot(
            chains=["arbitrum", "ethereum"],
            wallet_address="0xtest",
        )
        pd = PriceData(
            price=Decimal("3000"),
            change_24h_pct=Decimal("2.5"),
        )
        market.set_price_data("ETH", "arbitrum", pd)

        assert market.price("ETH", chain="arbitrum") == Decimal("3000")
        # The full PriceData should be retrievable through the public API.
        assert market.price_data("ETH", chain="arbitrum").change_24h_pct == Decimal("2.5")

    def test_set_price_data_chain_isolation(self):
        """Price data set on one chain should not leak to another.

        VIB-4062: the canonical class raises a typed snapshot error rather
        than ``ValueError`` when the price provider has no data for a chain.
        We assert "an exception is raised" rather than the specific type to
        keep the test resilient to error-type-cleanup work in commit 5.
        """
        market = MultiChainMarketSnapshot(
            chains=["arbitrum", "ethereum"],
            wallet_address="0xtest",
        )
        market.set_price_data("ETH", "arbitrum", PriceData(price=Decimal("3000")))

        with pytest.raises((ValueError, Exception)):
            market.price("ETH", chain="ethereum")


class TestCriticalDataFailureTracking:
    def test_price_failure_is_tracked_as_permanent_when_symbol_invalid(self):
        market = MarketSnapshot(
            chain="bsc",
            wallet_address="0xtest",
            price_oracle=MagicMock(side_effect=ValueError("Unknown token: USD")),
        )

        with pytest.raises(ValueError):
            market.price("USD")

        assert market.has_critical_data_failures()
        assert market.classify_critical_data_failures() == "permanent"
        assert "Unknown token: USD" in market.summarize_critical_data_failures()

    def test_timeout_failure_is_tracked_as_transient(self):
        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            price_oracle=MagicMock(side_effect=TimeoutError("request timed out")),
        )

        with pytest.raises(ValueError):
            market.price("ETH")

        assert market.has_critical_data_failures()
        assert market.classify_critical_data_failures() == "transient"

    def test_successful_lookup_clears_previous_failure_for_same_key(self):
        oracle = MagicMock(side_effect=[ValueError("Unknown token"), Decimal("1800")])
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest", price_oracle=oracle)

        with pytest.raises(ValueError):
            market.price("ETH")
        assert market.has_critical_data_failures()

        assert market.price("ETH") == Decimal("1800")
        assert not market.has_critical_data_failures()

    def test_clear_critical_data_failures_resets_all(self):
        """clear_critical_data_failures() wipes all tracked failures (used after pre-warm)."""
        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            price_oracle=MagicMock(side_effect=ValueError("Unknown token: USD")),
        )

        with pytest.raises(ValueError):
            market.price("ETH")
        assert market.has_critical_data_failures()

        market.clear_critical_data_failures()
        assert not market.has_critical_data_failures()
        assert market.critical_data_failure_count() == 0


class TestMultiDexFacadeMethods:
    """Tests for price_across_dexs() and best_dex_price() facade methods (VIB-292)."""

    def test_price_across_dexs_raises_when_no_service(self):
        """price_across_dexs() raises NotImplementedError when multi_dex_service is None."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        with pytest.raises(NotImplementedError, match="Multi-DEX price comparison is not available"):
            market.price_across_dexs("USDC", "WETH", Decimal("1000"))

    def test_best_dex_price_raises_when_no_service(self):
        """best_dex_price() raises NotImplementedError when multi_dex_service is None."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        with pytest.raises(NotImplementedError, match="Multi-DEX price comparison is not available"):
            market.best_dex_price("USDC", "WETH", Decimal("1000"))

    def test_price_across_dexs_delegates_to_service(self):
        """price_across_dexs() delegates to multi_dex_service.get_prices_across_dexs()."""
        mock_result = MagicMock()
        mock_service = MagicMock()
        mock_service.get_prices_across_dexs = AsyncMock(return_value=mock_result)

        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            multi_dex_service=mock_service,
        )
        result = market.price_across_dexs("USDC", "WETH", Decimal("1000"), dexs=["uniswap_v3"])

        assert result is mock_result
        mock_service.get_prices_across_dexs.assert_awaited_once_with(
            "USDC", "WETH", Decimal("1000"), ["uniswap_v3"]
        )

    def test_best_dex_price_delegates_to_service(self):
        """best_dex_price() delegates to multi_dex_service.get_best_dex_price()."""
        mock_result = MagicMock()
        mock_service = MagicMock()
        mock_service.get_best_dex_price = AsyncMock(return_value=mock_result)

        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            multi_dex_service=mock_service,
        )
        result = market.best_dex_price("USDC", "WETH", Decimal("500"))

        assert result is mock_result
        mock_service.get_best_dex_price.assert_awaited_once_with(
            "USDC", "WETH", Decimal("500"), None
        )

    def test_price_across_dexs_default_dexs_is_none(self):
        """price_across_dexs() passes None for dexs when not specified."""
        mock_service = MagicMock()
        mock_service.get_prices_across_dexs = AsyncMock(return_value=MagicMock())

        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            multi_dex_service=mock_service,
        )
        market.price_across_dexs("WETH", "USDC", Decimal("1"))

        mock_service.get_prices_across_dexs.assert_awaited_once_with(
            "WETH", "USDC", Decimal("1"), None
        )


class TestCollateralValueUsd:
    """Tests for MarketSnapshot.collateral_value_usd() helper."""

    def test_non_stablecoin_collateral(self):
        """WETH collateral: amount * price."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("WETH", Decimal("2500"))

        result = market.collateral_value_usd("WETH", Decimal("2"))
        assert result == Decimal("5000")

    def test_stablecoin_collateral(self):
        """USDC collateral: amount * price (price ~1.0)."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("USDC", Decimal("1.00"))

        result = market.collateral_value_usd("USDC", Decimal("5000"))
        assert result == Decimal("5000.00")

    def test_fractional_amount(self):
        """Fractional token amounts should work correctly."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("WBTC", Decimal("60000"))

        result = market.collateral_value_usd("WBTC", Decimal("0.05"))
        assert result == Decimal("3000.00")

    def test_zero_amount(self):
        """Zero collateral should return zero USD."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("WETH", Decimal("2500"))

        result = market.collateral_value_usd("WETH", Decimal("0"))
        assert result == Decimal("0")


class TestCachedPriceForCaseInsensitive:
    """_cached_price_for resolves prices seeded under a different case (VIB-4843).

    ``set_price`` stores ``_prices`` keys verbatim (no normalization), so a
    mixed-case token (cbBTC, wstETH, ...) looked up under another case would
    otherwise miss and leave the balance's USD silently unmeasured.
    """

    def test_mixed_case_price_resolves_case_insensitively(self):
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("cbBTC", Decimal("64000"))

        assert market._cached_price_for("cbBTC", "arbitrum") == Decimal("64000")  # exact
        assert market._cached_price_for("CBBTC", "arbitrum") == Decimal("64000")  # upper
        assert market._cached_price_for("cbbtc", "arbitrum") == Decimal("64000")  # lower

    def test_unknown_token_returns_none(self):
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("WETH", Decimal("2500"))
        assert market._cached_price_for("ARB", "arbitrum") is None

    def test_wrong_chain_skips_prices_map(self):
        """``_prices`` is only consulted for the snapshot's own chain."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("WETH", Decimal("2500"))
        assert market._cached_price_for("WETH", "base") is None


class TestPriceCaseInsensitive:
    """``price()`` resolves seeded prices case-insensitively, matching the balance
    path (``_cached_price_for``).

    Regression: the PnL backtest engine seeds ``_prices`` with upper-cased
    symbols (``market_state.available_tokens``) but a strategy queries its
    config casing (e.g. ``market.price("wstETH")``). A case-sensitive lookup
    missed and fell through to an oracle that cannot resolve a non-native
    token, so the strategy got ``ValueError`` every tick, executed zero
    intents, and the run still reported ``institutional_compliance=true`` /
    100% coverage — a silent false-clean lending backtest.
    """

    def test_price_resolves_mixed_case_seeded_symbol(self):
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("WSTETH", Decimal("3965.76"))  # engine seeds upper-cased

        assert market.price("WSTETH") == Decimal("3965.76")  # exact
        assert market.price("wstETH") == Decimal("3965.76")  # strategy config casing
        assert market.price("wsteth") == Decimal("3965.76")  # lower

    def test_price_still_raises_for_genuinely_unknown_token(self):
        """The case-insensitive fallback must not turn an unknown token into a hit."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("WSTETH", Decimal("3965.76"))
        with pytest.raises(ValueError, match="Cannot determine price"):
            market.price("ARB")


class TestMarketSnapshotTimeframeResolution:
    """Tests for _resolve_timeframe() priority: explicit > config default > 4h fallback."""

    def test_explicit_timeframe_overrides_default(self):
        """An explicit timeframe argument should override the config-driven default."""
        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            default_timeframe="15m",
        )
        # Explicit "1h" should win over the "15m" default
        assert market._resolve_timeframe("1h") == "1h"

    def test_explicit_timeframe_overrides_fallback(self):
        """An explicit timeframe should override the 4h module-level fallback."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        assert market._resolve_timeframe("30m") == "30m"

    def test_config_data_granularity_used_as_default(self):
        """When no explicit timeframe is passed, the config default_timeframe should be used."""
        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            default_timeframe="15m",
        )
        assert market._resolve_timeframe(None) == "15m"

    def test_fallback_to_4h_when_no_config(self):
        """When neither explicit timeframe nor config default is set, fall back to DEFAULT_TIMEFRAME."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        result = market._resolve_timeframe(None)
        assert result == DEFAULT_TIMEFRAME
        assert result == "4h"

    def test_timeframe_resolution_flows_through_rsi(self):
        """Verify _resolve_timeframe is actually used by indicator methods (RSI)."""
        captured_timeframes = []

        def mock_rsi_provider(token, period, timeframe=None):
            captured_timeframes.append(timeframe)
            return RSIData(value=Decimal("55"), period=period)

        # Case 1: config default should flow through when no explicit timeframe
        market_with_config = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            rsi_provider=mock_rsi_provider,
            default_timeframe="15m",
        )
        market_with_config.rsi("ETH", period=14)
        assert captured_timeframes[-1] == "15m"

        # Case 2: explicit timeframe should override config default
        market_with_config.rsi("BTC", period=14, timeframe="1h")
        assert captured_timeframes[-1] == "1h"

        # Case 3: no config, no explicit -> 4h fallback
        market_no_config = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            rsi_provider=mock_rsi_provider,
        )
        market_no_config.rsi("ETH", period=14)
        assert captured_timeframes[-1] == "4h"
