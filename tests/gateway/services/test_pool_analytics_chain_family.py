"""Family-based chain branching helpers in pool_analytics_service.py (VIB-4855).

Pre-W3 the gateway pool analytics service branched on the chain-name
string literal ``"solana"`` in four places (pool-address normalization,
pool-address validation, DefiLlama target-address derivation, DefiLlama
pool-id address-segment normalization). W3 consolidates all four into a
single ``_is_solana_family`` helper that consults
:class:`ChainRegistry` and compares on :class:`ChainFamily`. These tests
pin the helper + its two public consumers
(``_normalize_pool_address``, ``_validate_pool_address``).
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.gateway.services.pool_analytics_service import (
    _is_solana_family,
    _normalize_pool_address,
    _validate_pool_address,
)

# Real on-chain pool addresses; using literals keeps the tests
# self-contained.
SOL_POOL_RAYDIUM_USDC_SOL = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtL45dKtnGYWctY"
EVM_POOL_UNI_V3_MIXED_CASE = "0xC6962004f452bE9203591991D15f6b388e09E8D0"
EVM_POOL_UNI_V3_LOWER = EVM_POOL_UNI_V3_MIXED_CASE.lower()


class TestIsSolanaFamily:
    """The pool-service-local ``_is_solana_family`` helper is the chokepoint."""

    def test_solana_canonical_name_is_solana_family(self) -> None:
        assert _is_solana_family("solana") is True

    @pytest.mark.parametrize(
        "chain",
        ["arbitrum", "ethereum", "base", "polygon", "bsc", "avalanche", "optimism"],
    )
    def test_evm_chains_are_not_solana_family(self, chain: str) -> None:
        assert _is_solana_family(chain) is False

    def test_unknown_chain_falls_through_to_false(self) -> None:
        # Matches the legacy contract: an unknown chain is NOT Solana,
        # so callers default to EVM-style normalization.
        assert _is_solana_family("not-a-real-chain") is False

    def test_empty_chain_returns_false(self) -> None:
        assert _is_solana_family("") is False


class TestNormalizePoolAddress:
    """Address normalization branches on chain family."""

    def test_solana_pool_address_case_preserved(self) -> None:
        normalized = _normalize_pool_address(SOL_POOL_RAYDIUM_USDC_SOL, "solana")
        assert normalized == SOL_POOL_RAYDIUM_USDC_SOL

    def test_solana_pool_address_whitespace_stripped(self) -> None:
        # Leading/trailing whitespace from a copy-paste must still
        # round-trip — only the surrounding whitespace is stripped, the
        # base58 body's case is preserved.
        normalized = _normalize_pool_address(f"  {SOL_POOL_RAYDIUM_USDC_SOL}  ", "solana")
        assert normalized == SOL_POOL_RAYDIUM_USDC_SOL

    def test_evm_pool_address_lowercased(self) -> None:
        normalized = _normalize_pool_address(EVM_POOL_UNI_V3_MIXED_CASE, "arbitrum")
        assert normalized == EVM_POOL_UNI_V3_LOWER

    def test_unknown_chain_treated_as_evm(self) -> None:
        # The legacy contract was "anything not 'solana' is lowercased";
        # we preserve it exactly.
        normalized = _normalize_pool_address(EVM_POOL_UNI_V3_MIXED_CASE, "not-a-real-chain")
        assert normalized == EVM_POOL_UNI_V3_LOWER


class TestValidatePoolAddress:
    """Pool-address validation regex selection branches on chain family."""

    def test_solana_pool_passes_on_solana(self) -> None:
        assert _validate_pool_address(SOL_POOL_RAYDIUM_USDC_SOL, "solana") is True

    def test_evm_pool_rejected_on_solana(self) -> None:
        # 0x-prefixed hex is not a valid base58 mint — must reject.
        assert _validate_pool_address(EVM_POOL_UNI_V3_LOWER, "solana") is False

    def test_evm_pool_passes_on_evm(self) -> None:
        assert _validate_pool_address(EVM_POOL_UNI_V3_LOWER, "arbitrum") is True

    def test_evm_pool_mixed_case_rejected_on_evm(self) -> None:
        # The EVM validator requires the address to be lowercased
        # already — its regex is ``^0x[0-9a-f]{40}$``. Mixed case
        # therefore fails; this is the pre-W3 contract and the helper
        # refactor must not have weakened it.
        assert _validate_pool_address(EVM_POOL_UNI_V3_MIXED_CASE, "arbitrum") is False

    def test_solana_pool_rejected_on_evm(self) -> None:
        # Base58 with 0/O/I/l excluded still might pass the EVM regex?
        # No — no 0x prefix. Confirm.
        assert _validate_pool_address(SOL_POOL_RAYDIUM_USDC_SOL, "arbitrum") is False


class TestFamilyLockstepWithRegistry:
    """Service helper agrees with :class:`ChainRegistry`."""

    def test_helper_matches_registry_for_every_registered_chain(self) -> None:
        for descriptor in ChainRegistry.all():
            expected = descriptor.family is ChainFamily.SOLANA
            assert _is_solana_family(descriptor.name) is expected, (
                f"{descriptor.name}: helper says solana={_is_solana_family(descriptor.name)}, "
                f"ChainRegistry says family={descriptor.family.name}"
            )
