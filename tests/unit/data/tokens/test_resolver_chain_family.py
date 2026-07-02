"""Family-based chain branching in framework/data/tokens/resolver.py (VIB-4855).

Before W3 these call sites branched on the chain-name string literal
``"solana"``. The migration routes them through
:func:`almanak.core.chains.ChainRegistry.try_resolve` and compares on
:class:`almanak.core.enums.ChainFamily`. These tests pin the new
semantics:

1. Solana-family chains keep base58 case in
   ``_normalize_address_for_chain`` and select the SPL base58 pattern in
   ``_is_address`` / ``_validate_address``.
2. EVM-family chains lowercase addresses and require the 0x-hex pattern.
3. Unknown chain names fall through to the EVM branch — this matches
   the legacy ``chain.lower() == "solana"`` contract of "anything that
   isn't 'solana' is treated as EVM".
4. The migrated check stays in lockstep with the canonical family
   lookup (``ChainRegistry.resolve(chain).family``) for every registered
   chain — no chain accidentally drops out of either side.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.data.tokens.exceptions import InvalidTokenAddressError
from almanak.framework.data.tokens.resolver import (
    _is_address,
    _is_solana_chain,
    _normalize_address_for_chain,
    _validate_address,
)

# Real on-chain addresses pulled from production registries; using
# constants from the resolver / docs keeps these tests independent of the
# token registry's contents.
SOL_MINT_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT_WSOL = "So11111111111111111111111111111111111111112"
EVM_ADDR_MIXED_CASE = "0xAf88d065e77c8cC2239327C5EDb3A432268e5831"  # USDC on Arbitrum (EIP-55)
EVM_ADDR_LOWER = EVM_ADDR_MIXED_CASE.lower()


class TestIsSolanaChain:
    """``_is_solana_chain`` is the W3 chokepoint helper."""

    def test_solana_canonical_name_is_solana_family(self) -> None:
        assert _is_solana_chain("solana") is True

    def test_solana_uppercase_is_solana_family(self) -> None:
        # ChainRegistry.try_resolve normalizes case; the helper must
        # tolerate any case in the input.
        assert _is_solana_chain("SOLANA") is True
        assert _is_solana_chain("Solana") is True

    @pytest.mark.parametrize(
        "chain",
        ["arbitrum", "ethereum", "base", "polygon", "bsc", "avalanche"],
    )
    def test_evm_chains_are_not_solana_family(self, chain: str) -> None:
        assert _is_solana_chain(chain) is False

    def test_unknown_chain_falls_through_to_false(self) -> None:
        # Matches the legacy contract: unknown chains are NOT Solana, so
        # the EVM branch handles them (and fails downstream with the
        # right error).
        assert _is_solana_chain("not-a-real-chain") is False

    def test_empty_string_returns_false(self) -> None:
        assert _is_solana_chain("") is False

    def test_none_returns_false(self) -> None:
        assert _is_solana_chain(None) is False


class TestNormalizeAddressForChain:
    """W3 call site: address normalization branches on chain family."""

    def test_solana_address_case_preserved(self) -> None:
        # Solana base58 is case-sensitive — lowercasing yields a
        # different (invalid) address.
        normalized = _normalize_address_for_chain(SOL_MINT_USDC, "solana")
        assert normalized == SOL_MINT_USDC

    def test_evm_address_lowercased(self) -> None:
        # EVM hex is case-insensitive; we lowercase for cache-key
        # stability.
        normalized = _normalize_address_for_chain(EVM_ADDR_MIXED_CASE, "arbitrum")
        assert normalized == EVM_ADDR_LOWER

    def test_evm_address_lowercased_on_every_registered_evm_chain(self) -> None:
        for descriptor in ChainRegistry.all():
            if descriptor.family is ChainFamily.EVM:
                got = _normalize_address_for_chain(EVM_ADDR_MIXED_CASE, descriptor.name)
                assert got == EVM_ADDR_LOWER, (
                    f"{descriptor.name}: expected EVM-family address to be "
                    f"lowercased, got {got!r}"
                )


class TestIsAddressFamilyBranching:
    """``_is_address`` uses the family check to pick the address regex."""

    def test_solana_mint_recognized_on_solana_chain(self) -> None:
        assert _is_address(SOL_MINT_USDC, "solana") is True
        assert _is_address(SOL_MINT_WSOL, "solana") is True

    def test_solana_mint_rejected_on_evm_chain(self) -> None:
        # On an EVM chain the SOLANA base58 mint is not a 0x-hex
        # address — must fail the EVM regex.
        assert _is_address(SOL_MINT_USDC, "arbitrum") is False
        assert _is_address(SOL_MINT_USDC, "ethereum") is False

    def test_evm_address_recognized_on_evm_chain(self) -> None:
        assert _is_address(EVM_ADDR_MIXED_CASE, "arbitrum") is True
        assert _is_address(EVM_ADDR_LOWER, "arbitrum") is True

    def test_evm_address_rejected_on_solana_chain(self) -> None:
        # On Solana the 0x-hex form is not a valid SPL mint.
        assert _is_address(EVM_ADDR_MIXED_CASE, "solana") is False

    def test_unknown_chain_uses_evm_pattern(self) -> None:
        # Unknown chain → falls through the SOLANA branch → tries EVM
        # regex. EVM address matches, SOL mint does not (no chain
        # context to enable the SOLANA path).
        assert _is_address(EVM_ADDR_MIXED_CASE, "not-a-real-chain") is True


class TestValidateAddressFamilyBranching:
    """``_validate_address`` raises ``InvalidTokenAddressError`` on mismatch."""

    def test_valid_solana_mint_passes_on_solana(self) -> None:
        _validate_address(SOL_MINT_USDC, "solana")  # no raise

    def test_evm_address_on_solana_raises(self) -> None:
        with pytest.raises(InvalidTokenAddressError):
            _validate_address(EVM_ADDR_MIXED_CASE, "solana")

    def test_valid_evm_address_passes_on_evm(self) -> None:
        _validate_address(EVM_ADDR_MIXED_CASE, "arbitrum")  # no raise

    def test_solana_mint_on_evm_raises(self) -> None:
        with pytest.raises(InvalidTokenAddressError):
            _validate_address(SOL_MINT_USDC, "arbitrum")


class TestFamilyLockstepInvariant:
    """The migrated helper agrees with the canonical ``ChainRegistry`` lookup.

    This is the cross-check that catches a future drift between
    ``_is_solana_chain`` and the W3 source of truth — without it, a
    rename or refactor of the helper could silently break a single call
    site while every targeted test still passes.
    """

    def test_helper_matches_registry_for_every_registered_chain(self) -> None:
        for descriptor in ChainRegistry.all():
            expected = descriptor.family is ChainFamily.SOLANA
            assert _is_solana_chain(descriptor.name) is expected, (
                f"{descriptor.name}: helper says solana={_is_solana_chain(descriptor.name)}, "
                f"ChainRegistry says family={descriptor.family.name}"
            )

    def test_helper_matches_registry_for_aliases(self) -> None:
        # Every alias must resolve identically to its canonical chain.
        for alias, canonical in ChainRegistry.aliases().items():
            descriptor = ChainRegistry.get(canonical)
            expected = descriptor.family is ChainFamily.SOLANA
            assert _is_solana_chain(alias) is expected, (
                f"alias {alias!r} (chain {canonical}): helper says "
                f"solana={_is_solana_chain(alias)}, registry family={descriptor.family.name}"
            )
