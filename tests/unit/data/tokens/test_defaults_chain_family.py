"""Family-based sentinel cross-check gate in tokens/defaults.py (VIB-4855).

The module-load-time sentinel cross-check now consults
:class:`ChainRegistry` to decide which entries in ``chains.json`` are
EVM-family (and must therefore obey the canonical EVM
``NATIVE_SENTINEL``). These tests pin the new gate
(``_is_evm_chain_for_sentinel_check``) and verify the legacy
``chain != "solana"`` semantics survive byte-for-byte.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.data.tokens.defaults import _is_evm_chain_for_sentinel_check


class TestIsEvmChainForSentinelCheck:
    """Pin the import-time helper."""

    @pytest.mark.parametrize(
        "chain",
        ["ethereum", "arbitrum", "base", "polygon", "bsc", "avalanche", "optimism"],
    )
    def test_registered_evm_chains_are_evm(self, chain: str) -> None:
        assert _is_evm_chain_for_sentinel_check(chain) is True

    def test_solana_is_not_evm(self) -> None:
        assert _is_evm_chain_for_sentinel_check("solana") is False

    def test_unknown_chain_conservatively_treated_as_evm(self) -> None:
        # The legacy contract was ``chain != "solana"`` — anything that
        # isn't ``"solana"`` fell into the sentinel cross-check. The
        # helper preserves that: unknown chains return True so the
        # cross-check still fires on them (a preview chain added to
        # chains.json before being registered in core.chains should
        # surface in the cross-check, not silently skip it).
        assert _is_evm_chain_for_sentinel_check("not-a-real-chain") is True


class TestFamilyLockstepWithRegistry:
    """Helper agrees with :class:`ChainRegistry` for every registered chain."""

    def test_helper_matches_registry_for_every_registered_chain(self) -> None:
        for descriptor in ChainRegistry.all():
            expected = descriptor.family is ChainFamily.EVM
            assert _is_evm_chain_for_sentinel_check(descriptor.name) is expected, (
                f"{descriptor.name}: helper says evm="
                f"{_is_evm_chain_for_sentinel_check(descriptor.name)}, "
                f"registry says family={descriptor.family.name}"
            )
