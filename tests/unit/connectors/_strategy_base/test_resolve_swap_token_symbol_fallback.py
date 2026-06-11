"""VIB-4978: canonical swap-token-symbol resolution + casing policy.

Pins the shared helpers the 4 address-emitting SWAP receipt parsers (Aerodrome,
PancakeSwap V3, SushiSwap V3, Uniswap V4) use to stamp the ledger token identity.
The contract is: the ledger receives a canonical UPPER-CASE symbol, never a raw
contract address, with a deterministic address fallback on resolver miss.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.base import (
    resolve_swap_token_symbol,
    resolve_swap_token_symbol_with_fallback,
)

# Real Arbitrum addresses that the bundled token catalogue resolves.
WETH_ARB = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
# An UNRESOLVABLE Solana-shaped base58 mint (case-sensitive — the fallback must
# preserve case verbatim; upper-casing would corrupt the mint).
SOL_UNKNOWN = "MixedCaseB58zzzQ9xKpLmNoPqRsTuVwXyZ1234567ab"


class TestResolveSwapTokenSymbol:
    """Shape-A parsers (Uniswap V4, PancakeSwap V3) — address in, symbol out."""

    def test_address_resolves_to_upper_symbol(self):
        assert resolve_swap_token_symbol(WETH_ARB, "arbitrum") == "WETH"
        assert resolve_swap_token_symbol(USDC_ARB, "arbitrum") == "USDC"

    def test_casing_policy_is_uppercase(self):
        # The ledger column is upper-cased to stay byte-identical to the
        # accounting FIFO/oracle canonical form (handler upper-cases too), so the
        # display value can never diverge from the basis key. Pin it.
        assert resolve_swap_token_symbol(WETH_ARB, "arbitrum").isupper()

    def test_unresolvable_address_falls_back_to_lowercased_address(self):
        unknown = "0x" + "ab" * 20
        assert resolve_swap_token_symbol(unknown, "arbitrum") == unknown.lower()

    def test_symbol_passthrough_uppercased(self):
        assert resolve_swap_token_symbol("weth", "arbitrum") == "WETH"

    def test_none_and_empty_passthrough(self):
        assert resolve_swap_token_symbol(None, "arbitrum") is None
        assert resolve_swap_token_symbol("", "arbitrum") == ""

    def test_solana_base58_case_preserved_on_fallback(self):
        # No 0x prefix, base58 is case-sensitive — an unresolvable mint must
        # round-trip unchanged (never upper-cased, which would corrupt it).
        assert resolve_swap_token_symbol(SOL_UNKNOWN, "solana") == SOL_UNKNOWN


class TestResolveSwapTokenSymbolWithFallback:
    """Shape-B parsers (Aerodrome, SushiSwap V3) — (symbol, address, hint) trio."""

    def test_symbol_wins_and_is_canonicalised(self):
        assert resolve_swap_token_symbol_with_fallback("weth", USDC_ARB, "", "arbitrum") == "WETH"

    def test_empty_symbol_resolves_address(self):
        assert resolve_swap_token_symbol_with_fallback("", WETH_ARB, "", "arbitrum") == "WETH"

    def test_empty_symbol_and_address_resolves_hint(self):
        assert resolve_swap_token_symbol_with_fallback("", "", USDC_ARB, "arbitrum") == "USDC"

    def test_all_empty_returns_empty(self):
        assert resolve_swap_token_symbol_with_fallback("", "", "", "arbitrum") == ""

    def test_unresolvable_trio_falls_back_to_lowercased_address(self):
        unknown = "0x" + "cd" * 20
        assert resolve_swap_token_symbol_with_fallback("", unknown, "", "arbitrum") == unknown.lower()
