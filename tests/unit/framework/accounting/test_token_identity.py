"""Tests for the read-side token identity canonicalization helper (W1-4 / VIB-4779).

Covers:
1. Symbol-form input → CanonicalToken with address resolved.
2. Address-form input → CanonicalToken with symbol resolved.
3. Key invariant: symbol-form == address-form for the same token.
4. Mixed-case EVM address → identical canonical form.
5. Chain-prefixed input resolves with the prefix chain.
6. Unknown token fallbacks (fallback_to_input=True / False).
7. Solana address → case preserved, no lowercasing.
8. Empty input → None.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from almanak.framework.accounting.token_identity import (
    canonicalize_token_for_read,
)

# Full dotted path for patching the lazy import inside canonicalize_token_for_read.
_RESOLVER_PATCH = "almanak.framework.data.tokens.get_token_resolver"

# ── Constants used across tests ───────────────────────────────────────────────
# Native USDC on Arbitrum (Circle-issued, non-bridged).
_USDC_ARBITRUM_ADDR = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"  # lowercased
_USDC_ARBITRUM_ADDR_MIXED = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # mixed-case
_USDC_SYMBOL = "USDC"
_CHAIN = "arbitrum"

# Solana WSOL mint address (well-known, base58, case-sensitive).
_WSOL_MINT = "So11111111111111111111111111111111111111112"
_SOL_CHAIN = "solana"


# ── Helper: build a mock ResolvedToken ────────────────────────────────────────


def _mock_resolved(symbol: str, address: str, chain: str = _CHAIN) -> MagicMock:
    rt = MagicMock()
    rt.symbol = symbol
    rt.address = address  # resolver already normalizes to lowercase for EVM
    rt.chain = chain
    return rt


# ── Test cases ────────────────────────────────────────────────────────────────


class TestCanonicalizeTokenForRead:
    """Unit tests for canonicalize_token_for_read()."""

    # ── 1. Symbol-form resolution ──────────────────────────────────────────────

    def test_symbol_form_resolves_to_canonical_token(self) -> None:
        """Symbol-form 'USDC' on arbitrum should resolve to CanonicalToken with
        the lowercased address filled in."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = _mock_resolved(symbol=_USDC_SYMBOL, address=_USDC_ARBITRUM_ADDR)
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            result = canonicalize_token_for_read(_USDC_SYMBOL, _CHAIN)

        assert result is not None
        assert result.chain == _CHAIN
        assert result.address == _USDC_ARBITRUM_ADDR
        assert result.symbol == _USDC_SYMBOL

    # ── 2. Address-form resolution ─────────────────────────────────────────────

    def test_address_form_resolves_to_canonical_token(self) -> None:
        """Address-form on arbitrum should resolve to CanonicalToken with symbol
        filled in from the resolver."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = _mock_resolved(symbol=_USDC_SYMBOL, address=_USDC_ARBITRUM_ADDR)
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            result = canonicalize_token_for_read(_USDC_ARBITRUM_ADDR_MIXED, _CHAIN)

        assert result is not None
        assert result.chain == _CHAIN
        assert result.address == _USDC_ARBITRUM_ADDR
        assert result.symbol == _USDC_SYMBOL

    # ── 3. Key invariant: symbol-form == address-form (W1-4 acceptance) ────────

    def test_symbol_and_address_form_are_equal(self) -> None:
        """The W1-4 acceptance invariant: canonicalize_token_for_read('USDC', 'arbitrum')
        must equal canonicalize_token_for_read('0xaf88...', 'arbitrum')."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = _mock_resolved(symbol=_USDC_SYMBOL, address=_USDC_ARBITRUM_ADDR)
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            result_symbol = canonicalize_token_for_read(_USDC_SYMBOL, _CHAIN)
            result_address = canonicalize_token_for_read(_USDC_ARBITRUM_ADDR_MIXED, _CHAIN)

        assert result_symbol is not None
        assert result_address is not None
        assert result_symbol == result_address, (
            f"W1-4 invariant violated: symbol-form {result_symbol!r} != address-form {result_address!r}"
        )

    # ── 4. Mixed-case address → same result ───────────────────────────────────

    def test_mixed_case_address_canonicalizes_identically(self) -> None:
        """Upper and lower case variants of the same EVM address must produce
        the same CanonicalToken (equal by __eq__)."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = _mock_resolved(symbol=_USDC_SYMBOL, address=_USDC_ARBITRUM_ADDR)
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            # Fully lowercased vs mixed case — both should produce the same canonical.
            result_lower = canonicalize_token_for_read(_USDC_ARBITRUM_ADDR, _CHAIN)
            result_mixed = canonicalize_token_for_read(_USDC_ARBITRUM_ADDR_MIXED, _CHAIN)

        assert result_lower == result_mixed

    # ── 5. Chain-prefix form ──────────────────────────────────────────────────

    def test_chain_prefix_resolves_using_prefix_chain(self) -> None:
        """'arbitrum:0xaf88...' with chain='' should resolve using the prefix chain."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = _mock_resolved(symbol=_USDC_SYMBOL, address=_USDC_ARBITRUM_ADDR)
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            prefixed = f"{_CHAIN}:{_USDC_ARBITRUM_ADDR_MIXED}"
            result = canonicalize_token_for_read(prefixed, "")

        assert result is not None
        assert result.chain == _CHAIN
        assert result.address == _USDC_ARBITRUM_ADDR
        # The resolver was called with the stripped identifier and the prefix chain.
        mock_resolver.resolve.assert_called_once_with(
            _USDC_ARBITRUM_ADDR_MIXED, _CHAIN, log_errors=False, skip_gateway=True
        )

    # ── 6. Unknown token fallback ─────────────────────────────────────────────

    def test_unknown_address_fallback_true_returns_salvaged_canonical(self) -> None:
        """When resolver raises and fallback_to_input=True, an address input
        should return a CanonicalToken with address=lower(input), symbol=''."""
        unknown_addr = "0x1234567890abcdef1234567890abcdef12345678"

        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("token not found")
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            result = canonicalize_token_for_read(unknown_addr, _CHAIN, fallback_to_input=True)

        assert result is not None
        assert result.chain == _CHAIN
        assert result.address == unknown_addr.lower()
        assert result.symbol == ""

    def test_unknown_symbol_fallback_true_returns_salvaged_canonical(self) -> None:
        """When resolver raises and fallback_to_input=True, a symbol input
        should return a CanonicalToken with address='', symbol=upper(input)."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("token not found")
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            result = canonicalize_token_for_read("unknownTKN", _CHAIN, fallback_to_input=True)

        assert result is not None
        assert result.chain == _CHAIN
        assert result.address == ""
        assert result.symbol == "UNKNOWNTKN"

    def test_unknown_token_fallback_false_returns_none(self) -> None:
        """When resolver raises and fallback_to_input=False, None must be returned."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("token not found")
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            result = canonicalize_token_for_read("USDC", _CHAIN, fallback_to_input=False)
        assert result is None

    # ── 7. Solana address → case preserved ───────────────────────────────────

    def test_solana_address_not_lowercased(self) -> None:
        """Solana base58 addresses are case-sensitive and must not be lowercased."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("not in static registry")
        with patch(_RESOLVER_PATCH, return_value=mock_resolver):
            result = canonicalize_token_for_read(_WSOL_MINT, _SOL_CHAIN, fallback_to_input=True)

        assert result is not None
        assert result.chain == _SOL_CHAIN
        # Address must NOT be lowercased; must equal the original mint string.
        assert result.address == _WSOL_MINT, f"Solana address should preserve case. Got: {result.address!r}"
        assert result.symbol == ""

    # ── 8. Empty input → None ─────────────────────────────────────────────────

    def test_empty_token_returns_none(self) -> None:
        """Empty string input must return None regardless of fallback_to_input."""
        assert canonicalize_token_for_read("", "arbitrum") is None
        assert canonicalize_token_for_read("  ", "arbitrum") is None
        assert canonicalize_token_for_read("", "arbitrum", fallback_to_input=False) is None
