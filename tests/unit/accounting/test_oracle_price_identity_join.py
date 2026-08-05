"""Identity-form joins in the accounting oracle lookups (token-identity PR #3612).

Covers the address-aware probe ladders added to
``lending_reads._resolve_oracle_price`` and
``quant_aggregations._price_entry_for_token``: both join address-form asset
references against symbol-keyed / composite-keyed price dicts, and both stay
Empty ≠ Zero — an unresolvable reference is absent, never fabricated.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.accounting.lending_reads import _resolve_oracle_price
from almanak.framework.dashboard.quant_aggregations import _price_entry_for_token

# Canonical USDC on Base — resolvable by the offline static registry.
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
UNKNOWN_ADDR = "0x" + "d" * 40


class TestResolveOraclePrice:
    def test_exact_symbol_key(self) -> None:
        assert _resolve_oracle_price({"USDC": Decimal("1")}, "USDC") == Decimal("1")

    def test_case_insensitive_symbol_key(self) -> None:
        assert _resolve_oracle_price({"wstETH": Decimal("4000")}, "WSTETH") == Decimal("4000")

    def test_nested_shape(self) -> None:
        assert _resolve_oracle_price({"USDC": {"price_usd": "1.0"}}, "USDC") == Decimal("1.0")

    def test_address_joins_composite_key(self) -> None:
        oracle = {f"base:{USDC_BASE.lower()}": Decimal("0.999")}
        assert _resolve_oracle_price(oracle, USDC_BASE) == Decimal("0.999")

    def test_address_resolves_to_symbol_key_via_composite_chain_probe(self) -> None:
        # Symbol-keyed entry plus an unrelated composite key that names the
        # chain — the resolver bridges the bare address through that chain.
        oracle = {"USDC": Decimal("1.0"), f"base:{UNKNOWN_ADDR}": Decimal("5")}
        assert _resolve_oracle_price(oracle, USDC_BASE) == Decimal("1.0")

    def test_uppercase_0X_caller_input_joins_composite_key(self) -> None:
        oracle = {f"base:{USDC_BASE.lower()}": Decimal("0.999")}
        upper_input = "0X" + USDC_BASE[2:].upper()
        assert _resolve_oracle_price(oracle, upper_input) == Decimal("0.999")

    def test_unresolvable_address_stays_unpriced(self) -> None:
        assert _resolve_oracle_price({"USDC": Decimal("1")}, UNKNOWN_ADDR) is None

    def test_missing_symbol_stays_unpriced(self) -> None:
        assert _resolve_oracle_price({"USDC": Decimal("1")}, "NOPE") is None

    def test_none_oracle(self) -> None:
        assert _resolve_oracle_price(None, "USDC") is None


class TestPriceEntryForToken:
    def test_exact_key(self) -> None:
        prices = {"USDC": {"price_usd": "1"}}
        assert _price_entry_for_token(prices, "USDC") == {"price_usd": "1"}

    def test_case_insensitive_key(self) -> None:
        prices = {"wstETH": {"price_usd": "4000"}}
        assert _price_entry_for_token(prices, "WSTETH") == {"price_usd": "4000"}

    def test_address_joins_composite_key(self) -> None:
        prices = {f"BASE:{USDC_BASE.upper()}": {"price_usd": "1"}}
        assert _price_entry_for_token(prices, USDC_BASE.lower()) == {"price_usd": "1"}

    def test_address_resolves_to_symbol_key_via_composite_chain_probe(self) -> None:
        prices = {"USDC": {"price_usd": "1"}, f"base:{UNKNOWN_ADDR}": {"price_usd": "5"}}
        assert _price_entry_for_token(prices, USDC_BASE) == {"price_usd": "1"}

    def test_uppercase_0X_caller_input_joins_composite_key(self) -> None:
        prices = {f"base:{USDC_BASE.lower()}": {"price_usd": "1"}}
        upper_input = "0X" + USDC_BASE[2:].upper()
        assert _price_entry_for_token(prices, upper_input) == {"price_usd": "1"}

    def test_unresolvable_address_excluded(self) -> None:
        assert _price_entry_for_token({"USDC": {"price_usd": "1"}}, UNKNOWN_ADDR) is None

    def test_non_dict_entry_rejected(self) -> None:
        assert _price_entry_for_token({"USDC": "not-a-dict"}, "USDC") is None

    def test_non_string_token(self) -> None:
        assert _price_entry_for_token({"USDC": {"price_usd": "1"}}, None) is None