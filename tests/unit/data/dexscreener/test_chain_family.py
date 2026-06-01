"""Family-based chain branching in framework/data/dexscreener/client.py (VIB-4855).

After W3, ``_is_solana_chain_id`` gates every Solana-pair / Solana-boost
filter in :class:`DexScreenerClient`. These tests pin its contract:

* DexScreener's canonical chain names round-trip through
  :class:`ChainRegistry` and return SOLANA-family iff the chain is Solana.
* Unknown / missing / empty / ``None`` chain IDs always return ``False``
  (no AttributeError on a malformed upstream payload).
* The helper agrees with :class:`ChainRegistry` for every registered
  chain — a lockstep invariant that catches drift on either side.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.data.dexscreener.client import _is_solana_chain_id


class TestIsSolanaChainId:
    """Pin the DexScreener-side family helper."""

    def test_solana_string_returns_true(self) -> None:
        assert _is_solana_chain_id("solana") is True

    @pytest.mark.parametrize("chain_id", ["ethereum", "arbitrum", "base", "polygon", "bsc"])
    def test_evm_chain_ids_return_false(self, chain_id: str) -> None:
        assert _is_solana_chain_id(chain_id) is False

    def test_unknown_chain_id_returns_false(self) -> None:
        # DexScreener's API could one day add a chain we don't know;
        # we must not crash and must not misclassify it as Solana.
        assert _is_solana_chain_id("solana-l2-rollup") is False
        assert _is_solana_chain_id("some-future-chain") is False

    def test_none_chain_id_returns_false(self) -> None:
        # Defensive — a malformed DexScreener payload could surface
        # ``chain_id=None``; the helper must not raise ``AttributeError``.
        assert _is_solana_chain_id(None) is False

    def test_empty_chain_id_returns_false(self) -> None:
        assert _is_solana_chain_id("") is False

    def test_whitespace_chain_id_returns_false(self) -> None:
        # ``ChainRegistry.try_resolve`` strips/lowercases internally;
        # bare whitespace must not resolve to anything.
        assert _is_solana_chain_id("   ") is False


class TestFamilyLockstepWithRegistry:
    """The helper agrees with ``ChainRegistry`` for every registered chain."""

    def test_helper_matches_registry_for_every_registered_chain(self) -> None:
        for descriptor in ChainRegistry.all():
            expected = descriptor.family is ChainFamily.SOLANA
            assert _is_solana_chain_id(descriptor.name) is expected, (
                f"{descriptor.name}: helper says solana={_is_solana_chain_id(descriptor.name)}, "
                f"ChainRegistry says family={descriptor.family.name}"
            )
