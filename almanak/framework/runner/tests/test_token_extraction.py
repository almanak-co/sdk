"""Tests for the shared token extraction utility."""

from __future__ import annotations

from types import SimpleNamespace

from almanak.framework.runner.token_extraction import TOKEN_FIELDS, extract_token_symbols


class TestExtractTokenSymbols:
    """Tests for extract_token_symbols()."""

    def test_basic_swap_intent_object(self):
        intent = SimpleNamespace(from_token="USDC", to_token="ETH")
        assert extract_token_symbols(intent) == ["USDC", "ETH"]

    def test_basic_swap_intent_dict(self):
        intent = {"from_token": "USDC", "to_token": "ETH"}
        assert extract_token_symbols(intent) == ["USDC", "ETH"]

    def test_lending_fields(self):
        intent = {"borrow_token": "USDC", "collateral_token": "WETH"}
        assert extract_token_symbols(intent) == ["USDC", "WETH"]

    def test_lp_fields(self):
        intent = {"token_a": "USDC", "token_b": "WETH"}
        assert extract_token_symbols(intent) == ["USDC", "WETH"]

    def test_all_token_fields_covered(self):
        """Every field in TOKEN_FIELDS is extracted."""
        intent = {field: f"TOKEN_{i}" for i, field in enumerate(TOKEN_FIELDS)}
        result = extract_token_symbols(intent)
        assert len(result) == len(TOKEN_FIELDS)

    def test_skips_addresses(self):
        intent = {"from_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "to_token": "ETH"}
        assert extract_token_symbols(intent) == ["ETH"]

    def test_skips_empty_and_none(self):
        intent = {"from_token": "", "to_token": None, "token": "WBTC"}
        assert extract_token_symbols(intent) == ["WBTC"]

    def test_deduplicates_preserving_order(self):
        intent = {"from_token": "ETH", "to_token": "USDC", "token_in": "ETH", "token_out": "USDC"}
        assert extract_token_symbols(intent) == ["ETH", "USDC"]

    def test_recurses_into_callback_intents(self):
        intent = {
            "token": "WETH",
            "callback_intents": [
                {"from_token": "WETH", "to_token": "USDC"},
                {"from_token": "USDC", "to_token": "DAI"},
            ],
        }
        result = extract_token_symbols(intent)
        assert result == ["WETH", "USDC", "DAI"]

    def test_callback_intents_as_objects(self):
        cb1 = SimpleNamespace(from_token="WETH", to_token="USDC")
        cb2 = SimpleNamespace(from_token="USDC", to_token="DAI")
        intent = SimpleNamespace(token="WETH", callback_intents=[cb1, cb2])
        result = extract_token_symbols(intent)
        assert result == ["WETH", "USDC", "DAI"]

    def test_depth_guard_prevents_infinite_recursion(self):
        """Recursion stops at depth 3."""
        deep = {"token": "DEEP4"}
        for i in range(3, 0, -1):
            deep = {"token": f"LEVEL{i}", "callback_intents": [deep]}
        root = {"token": "ROOT", "callback_intents": [deep]}
        result = extract_token_symbols(root)
        # ROOT(0) -> LEVEL1(1) -> LEVEL2(2) -> LEVEL3(3) -> DEEP4 at depth 4 = skipped
        assert "ROOT" in result
        assert "LEVEL1" in result
        assert "LEVEL2" in result
        assert "LEVEL3" in result
        assert "DEEP4" not in result

    def test_no_callback_intents_field(self):
        intent = {"from_token": "ETH"}
        assert extract_token_symbols(intent) == ["ETH"]

    def test_empty_callback_intents(self):
        intent = {"from_token": "ETH", "callback_intents": []}
        assert extract_token_symbols(intent) == ["ETH"]

    def test_long_symbol_skipped(self):
        """Strings >= 20 chars are treated as non-symbols."""
        intent = {"from_token": "A" * 20}
        assert extract_token_symbols(intent) == []

    def test_19_char_symbol_included(self):
        intent = {"from_token": "A" * 19}
        assert extract_token_symbols(intent) == ["A" * 19]
