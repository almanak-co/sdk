"""Tests for the flexible price-map lookup in pnl_attributor.

``_price_for_token`` must accept both shapes the attribution lane sees:

- flat ``{symbol_or_address: price_str}`` (persisted on attribution_json)
- ``PortfolioSnapshot.token_prices`` shape:
  ``{"chain:0xaddr": {"price_usd": "...", "symbol": "..."}}``

and return ``None`` (never a fabricated zero) when the token is missing or
the price cannot be parsed — Empty != Zero.
"""

from decimal import Decimal

import pytest

from almanak.framework.observability.pnl_attributor import _price_for_token


class TestFlatPriceMap:
    def test_direct_symbol_match(self):
        assert _price_for_token({"USDC": "1.00"}, "USDC") == Decimal("1.00")

    def test_match_is_case_insensitive_both_ways(self):
        assert _price_for_token({"usdc": "1.00"}, "USDC") == Decimal("1.00")
        assert _price_for_token({"USDC": "1.00"}, "usdc") == Decimal("1.00")

    def test_numeric_value_parses(self):
        assert _price_for_token({"WETH": 3500.5}, "WETH") == Decimal("3500.5")

    def test_unparseable_flat_price_returns_none(self):
        assert _price_for_token({"USDC": "not-a-number"}, "USDC") is None

    def test_missing_token_returns_none(self):
        assert _price_for_token({"WETH": "3500"}, "USDC") is None

    @pytest.mark.parametrize(
        ("prices", "token"),
        [
            ({}, "USDC"),
            (None, "USDC"),
            ({"USDC": "1.00"}, ""),
            ({"USDC": "1.00"}, None),
        ],
    )
    def test_empty_inputs_return_none(self, prices, token):
        assert _price_for_token(prices, token) is None


class TestChainPrefixedKeys:
    def test_suffix_match_after_chain_prefix_flat_value(self):
        prices = {"arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831": "0.999"}
        assert _price_for_token(prices, "0xAF88d065e77c8cC2239327C5EDb3A432268e5831") == Decimal("0.999")

    def test_suffix_match_with_snapshot_dict_value(self):
        prices = {"arbitrum:0xaf88": {"price_usd": "1.01", "symbol": "USDC"}}
        assert _price_for_token(prices, "0xaf88") == Decimal("1.01")

    def test_partial_suffix_without_colon_does_not_match(self):
        """Only a full ``chain:token`` segment matches — no substring hits."""
        prices = {"arbitrum:0xdeadaf88": "5"}
        assert _price_for_token(prices, "0xaf88") is None


class TestSnapshotShape:
    def test_symbol_field_match(self):
        prices = {"arbitrum:0xaf88": {"price_usd": "1.02", "symbol": "USDC"}}
        assert _price_for_token(prices, "usdc") == Decimal("1.02")

    def test_direct_key_dict_without_parseable_price_falls_through(self):
        """A direct-key hit with a broken price must not shadow a later
        symbol-field match for the same token."""
        prices = {
            "weth": {"price_usd": "n/a", "symbol": "WETH"},
            "arbitrum:0x82af": {"price_usd": "3500", "symbol": "WETH"},
        }
        assert _price_for_token(prices, "WETH") == Decimal("3500")

    def test_dict_missing_price_usd_returns_none(self):
        prices = {"arbitrum:0xaf88": {"symbol": "USDC"}}
        assert _price_for_token(prices, "USDC") is None

    def test_symbol_match_with_unparseable_price_returns_none(self):
        prices = {"arbitrum:0xaf88": {"price_usd": "??", "symbol": "USDC"}}
        assert _price_for_token(prices, "USDC") is None

    def test_dict_without_symbol_field_is_skipped(self):
        prices = {"arbitrum:0xaf88": {"price_usd": "1.00"}}
        assert _price_for_token(prices, "USDC") is None
