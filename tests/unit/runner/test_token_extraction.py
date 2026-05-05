"""Tests for the shared token extraction utility."""

from __future__ import annotations

from types import SimpleNamespace

from almanak.framework.runner.token_extraction import (
    TOKEN_FIELDS,
    extract_token_symbols,
    parse_pool_tokens,
)


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


class TestParsePoolTokens:
    """Tests for parse_pool_tokens()."""

    def test_v3_fee_tier_pool(self):
        assert parse_pool_tokens("WETH/USDC/500") == ["WETH", "USDC"]

    def test_v3_fee_tier_3000(self):
        assert parse_pool_tokens("WETH/USDC/3000") == ["WETH", "USDC"]

    def test_two_token_pool(self):
        assert parse_pool_tokens("WETH/USDC") == ["WETH", "USDC"]

    def test_aerodrome_volatile_suffix_filtered(self):
        """Regression: 'volatile' must not be treated as a token symbol."""
        assert parse_pool_tokens("WETH/USDC/volatile") == ["WETH", "USDC"]

    def test_solidly_stable_suffix_filtered(self):
        assert parse_pool_tokens("USDC/USDT/stable") == ["USDC", "USDT"]

    def test_concentrated_suffix_filtered(self):
        assert parse_pool_tokens("WETH/USDC/concentrated") == ["WETH", "USDC"]

    def test_cl_suffix_filtered(self):
        """Aerodrome Slipstream 'cl' suffix."""
        assert parse_pool_tokens("WETH/USDC/cl") == ["WETH", "USDC"]

    def test_suffix_case_insensitive(self):
        assert parse_pool_tokens("WETH/USDC/VOLATILE") == ["WETH", "USDC"]
        assert parse_pool_tokens("WETH/USDC/Stable") == ["WETH", "USDC"]

    def test_bridged_token_alias_preserved(self):
        """Bridged aliases like USDC.e must pass through unchanged."""
        assert parse_pool_tokens("WETH/USDC.e/500") == ["WETH", "USDC.e"]

    def test_volatile_as_leading_token_preserved(self):
        """Suffix filter is trailing-position-only.

        If a token happens to be named VOLATILE and appears at position 0 or
        1, it MUST be preserved — only the pool-type suffix position gets
        stripped.
        """
        assert parse_pool_tokens("VOLATILE/WETH/500") == ["VOLATILE", "WETH"]

    def test_volatile_as_middle_token_preserved(self):
        assert parse_pool_tokens("WETH/VOLATILE/500") == ["WETH", "VOLATILE"]

    def test_decoration_stripped(self):
        """'USDC (0.05%)' -> 'USDC'."""
        assert parse_pool_tokens("USDC (0.05%)/WETH/500") == ["USDC", "WETH"]

    def test_whitespace_stripped(self):
        assert parse_pool_tokens(" WETH / USDC / 500 ") == ["WETH", "USDC"]

    def test_non_string_returns_empty(self):
        assert parse_pool_tokens(None) == []  # type: ignore[arg-type]
        assert parse_pool_tokens(123) == []  # type: ignore[arg-type]

    def test_no_slash_returns_empty(self):
        """Bare strings like market IDs are not pool descriptors."""
        assert parse_pool_tokens("usdc_e") == []
        assert parse_pool_tokens("WETH") == []

    def test_empty_string_returns_empty(self):
        assert parse_pool_tokens("") == []

    def test_address_segments_skipped(self):
        """Segments that look like addresses (0x...) are not symbols."""
        assert parse_pool_tokens("0xabc/USDC") == ["USDC"]

    def test_extract_token_symbols_uses_filter(self):
        """End-to-end: a full LP intent with pool='WETH/USDC/volatile'."""
        intent = SimpleNamespace(pool="WETH/USDC/volatile")
        assert extract_token_symbols(intent) == ["WETH", "USDC"]

    def test_extract_token_symbols_aerodrome_cl_intent(self):
        intent = {"pool": "WETH/USDC/cl"}
        assert extract_token_symbols(intent) == ["WETH", "USDC"]
