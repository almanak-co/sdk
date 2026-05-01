"""Tests for teardown fallback price oracle behavior.

Validates that:
- _get_fallback_teardown_prices returns stablecoin fallbacks + retried major tokens
- _build_teardown_compiler merges fallback into partially-populated oracles
- allow_placeholder_prices stays False when fallback prices are available
- Empty oracle triggers fallback; non-empty oracle keeps its prices
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner


class TestGetFallbackTeardownPrices:
    """Tests for StrategyRunner._get_fallback_teardown_prices."""

    def test_returns_universal_stablecoin_fallbacks_when_no_market(self):
        # Market=None → no chain context → only the universal stablecoins.
        # Bridged variants (USDC.e, USDbC, …) MUST NOT leak in (VIB-3814):
        # advertising USDC.e on a chain that doesn't have it caused the
        # downstream resolver to time out probing it.
        result = StrategyRunner._get_fallback_teardown_prices(None)
        assert result is not None
        assert result["USDC"] == Decimal("1")
        assert result["USDT"] == Decimal("1")
        assert result["DAI"] == Decimal("1")
        assert "USDC.e" not in result
        assert "USDbC" not in result

    def test_returns_stablecoin_fallbacks_when_market_has_no_price(self):
        market = MagicMock(spec=[])  # no .price attribute, no chain
        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result is not None
        assert "USDC" in result
        # No volatile tokens since market.price is not available
        assert "ETH" not in result

    def test_bsc_market_excludes_bridged_usdc_variants(self):
        # VIB-3814: BSC has neither USDC.e nor USDbC on-chain; advertising
        # them as $1 leaked the symbols into the merged price_oracle and
        # downstream consumers (e.g. fee-tier heuristic) burned 240s probing
        # the resolver for them. The fallback must omit bridged variants
        # on chains where they don't exist.
        market = MagicMock(spec=["_chain"])
        market._chain = "bsc"
        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result is not None
        assert result["USDC"] == Decimal("1")
        assert "USDC.e" not in result
        assert "USDbC" not in result

    def test_arbitrum_market_includes_usdc_e(self):
        market = MagicMock(spec=["_chain"])
        market._chain = "arbitrum"
        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result["USDC.e"] == Decimal("1")

    def test_linea_and_mantle_exclude_bridged_usdc_variants(self):
        # VIB-3814 follow-up: Linea and Mantle have no USDC.e / USDbC entry in
        # ``symbol_aliases.json``. Advertising them at $1 would replicate the
        # BSC phantom-symbol bug — Gemini caught this discrepancy in
        # PR #1994 review when an earlier draft of the table included them.
        for chain in ("linea", "mantle"):
            market = MagicMock(spec=["_chain"])
            market._chain = chain
            result = StrategyRunner._get_fallback_teardown_prices(market)
            assert result["USDC"] == Decimal("1"), chain
            assert "USDC.e" not in result, chain
            assert "USDbC" not in result, chain

    def test_base_market_includes_usdbc(self):
        market = MagicMock(spec=["_chain"])
        market._chain = "base"
        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result["USDbC"] == Decimal("1")
        # USDC.e is Arbitrum/Optimism/Polygon/Avalanche/Berachain naming —
        # not Base, so it must NOT be present here.
        assert "USDC.e" not in result

    def test_retries_major_tokens_from_market(self):
        market = MagicMock()
        market.price.side_effect = lambda sym: {
            "ETH": Decimal("3500"),
            "WETH": Decimal("3500"),
        }.get(sym, None)

        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result["ETH"] == Decimal("3500")
        assert result["WETH"] == Decimal("3500")
        # WBTC is NOT in the unconditional fetch list — only native/wrapped tokens
        assert "WBTC" not in result
        called_symbols = [call.args[0] for call in market.price.call_args_list]
        assert "WBTC" not in called_symbols
        # Stablecoins still present
        assert result["USDC"] == Decimal("1")

    def test_skips_tokens_with_zero_price(self):
        market = MagicMock()
        market.price.return_value = Decimal("0")

        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert "ETH" not in result
        assert "USDC" in result  # stablecoins still there

    def test_skips_tokens_when_price_raises(self):
        market = MagicMock()
        market.price.side_effect = Exception("gateway down")

        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert "ETH" not in result
        assert "USDC" in result


class TestBuildTeardownCompilerPriceOracle:
    """Tests for the price oracle merging logic in _build_teardown_compiler."""

    def _make_runner(self):
        runner = MagicMock(spec=StrategyRunner)
        runner._get_fallback_teardown_prices = StrategyRunner._get_fallback_teardown_prices
        return runner

    def test_empty_oracle_gets_fallback(self):
        """When get_price_oracle_dict returns {}, fallback prices fill in."""
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        market.price.side_effect = lambda sym: Decimal("3500") if sym == "ETH" else None

        # Simulate the merge logic from _build_teardown_compiler
        fetched = market.get_price_oracle_dict()
        fallback = StrategyRunner._get_fallback_teardown_prices(market)
        merged = {**(fallback or {}), **(fetched or {})}
        price_oracle = merged or None

        assert price_oracle is not None
        assert "USDC" in price_oracle
        assert price_oracle["ETH"] == Decimal("3500")

    def test_nonempty_oracle_preserves_fetched_prices(self):
        """When get_price_oracle_dict has real prices, they take precedence."""
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {
            "ETH": Decimal("4000"),
            "USDC": Decimal("0.999"),
        }
        market.price.return_value = None

        fetched = market.get_price_oracle_dict()
        fallback = StrategyRunner._get_fallback_teardown_prices(market)
        merged = {**(fallback or {}), **(fetched or {})}
        price_oracle = merged or None

        # Fetched prices override fallback
        assert price_oracle["ETH"] == Decimal("4000")
        assert price_oracle["USDC"] == Decimal("0.999")
        # Fallback fills in missing tokens
        assert "DAI" in price_oracle

    def test_partial_oracle_gets_missing_tokens_from_fallback(self):
        """Partially populated oracle gets fallback for missing tokens."""
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {"USDC": Decimal("1")}
        market.price.side_effect = lambda sym: Decimal("3500") if sym == "WETH" else None

        fetched = market.get_price_oracle_dict()
        fallback = StrategyRunner._get_fallback_teardown_prices(market)
        merged = {**(fallback or {}), **(fetched or {})}
        price_oracle = merged or None

        assert price_oracle["USDC"] == Decimal("1")
        assert price_oracle["WETH"] == Decimal("3500")
        assert "DAI" in price_oracle

    def test_placeholder_prices_false_when_fallback_available(self):
        """allow_placeholder_prices should be False when fallback is available."""
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        market.price.return_value = None

        fetched = market.get_price_oracle_dict()
        fallback = StrategyRunner._get_fallback_teardown_prices(market)
        merged = {**(fallback or {}), **(fetched or {})}
        price_oracle = merged or None

        has_prices = bool(price_oracle)
        assert has_prices is True
        # This means allow_placeholder_prices=not has_prices == False
        assert (not has_prices) is False
