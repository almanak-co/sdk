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

USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH_ARB = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


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
    def test_chain_context_resolves_address_against_symbol_oracle(self):
        prices = {"WETH": "3500", "USDC": "1"}

        assert _price_for_token(prices, WETH_ARB, chain="arbitrum") == Decimal("3500")
        assert _price_for_token(prices, USDC_ARB, chain="arbitrum") == Decimal("1")

    def test_suffix_match_after_chain_prefix_flat_value(self):
        prices = {"arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831": "0.999"}
        assert _price_for_token(prices, "0xAF88d065e77c8cC2239327C5EDb3A432268e5831") == Decimal("0.999")

    def test_suffix_match_with_snapshot_dict_value(self):
        prices = {f"arbitrum:{USDC_ARB}": {"price_usd": "1.01", "symbol": "USDC"}}
        assert _price_for_token(prices, USDC_ARB) == Decimal("1.01")

    def test_partial_suffix_without_colon_does_not_match(self):
        """Only a full ``chain:token`` segment matches — no substring hits."""
        different_address_with_same_tail = "0x" + "de" * 4 + USDC_ARB[10:]
        prices = {f"arbitrum:{different_address_with_same_tail}": "5"}

        assert different_address_with_same_tail.endswith(USDC_ARB[10:])
        assert _price_for_token(prices, USDC_ARB) is None

    def test_cross_chain_address_collision_fails_closed_instead_of_choosing_first(self):
        prices = {
            f"arbitrum:{USDC_ARB}": "1",
            f"ethereum:{USDC_ARB}": "2000",
        }

        assert _price_for_token(prices, USDC_ARB) is None


class TestSnapshotShape:
    def test_symbol_field_match(self):
        prices = {f"arbitrum:{USDC_ARB}": {"price_usd": "1.02", "symbol": "USDC"}}
        assert _price_for_token(prices, "usdc") == Decimal("1.02")

    def test_direct_key_dict_without_parseable_price_falls_through(self):
        """A direct-key hit with a broken price must not shadow a later
        symbol-field match for the same token."""
        prices = {
            "weth": {"price_usd": "n/a", "symbol": "WETH"},
            "arbitrum:0x82af49447d8a07e3bd95bd0d56f35241523fbab1": {"price_usd": "3500", "symbol": "WETH"},
        }
        assert _price_for_token(prices, "WETH") == Decimal("3500")

    def test_dict_missing_price_usd_returns_none(self):
        prices = {f"arbitrum:{USDC_ARB}": {"symbol": "USDC"}}
        assert _price_for_token(prices, "USDC") is None

    def test_symbol_match_with_unparseable_price_returns_none(self):
        prices = {f"arbitrum:{USDC_ARB}": {"price_usd": "??", "symbol": "USDC"}}
        assert _price_for_token(prices, "USDC") is None

    def test_dict_without_symbol_field_uses_offline_identity_alias(self):
        prices = {f"arbitrum:{USDC_ARB}": {"price_usd": "1.00"}}
        assert _price_for_token(prices, "USDC") == Decimal("1.00")
