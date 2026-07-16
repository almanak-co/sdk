"""Equivalence tests for the CS-2 / CS-3 inversions (VIB-4851 Phase E).

Freezes the legacy literals verbatim and proves the registry-derived
replacements reproduce them — the B1 anti-widening discipline
(``tests/unit/core/test_external_ids_inversion.py`` template). Two
deliberate, documented widenings are pinned explicitly rather than
hidden: ``is_solana_chain`` accepting alias/cased inputs, and
``SUPPORTED_NORMALIZATION_CHAINS`` covering every registered chain.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import (
    evm_chain_names,
    fork_archive_required_chains,
    is_solana_chain,
    rpc_rate_limit_map,
    solana_chain_names,
)

# ── Frozen legacy literals (verbatim from the pre-CS-3 modules) ─────────────

FROZEN_CHAIN_RATE_LIMITS: dict[str, int] = {
    "ethereum": 300,
    "arbitrum": 300,
    "base": 300,
    "optimism": 300,
    "polygon": 300,
    "avalanche": 300,
    "bsc": 300,
    "sonic": 300,
    "plasma": 300,
    "solana": 300,
}

FROZEN_ARCHIVE_RPC_REQUIRED_CHAINS = frozenset(
    {"polygon", "ethereum", "avalanche", "zerog", "xlayer"}
)

# DELIBERATE widening (VIB-5869 / ALM-2695), pinned here rather than hidden.
# The frozen literal above was not a considered design — it was the residue of
# three separate incidents (VIB-646, VIB-3971, VIB-3973), and it ran INVERTED
# to measured risk: ethereum's public RPC serves the longest state window
# (~19min) and was flagged, while arbitrum (~16s) and bsc (~48s) were not, so
# a managed BSC fork started fine and died minutes later on `missing trie
# node`. Membership is now derived from the measured retention table in
# ``almanak/core/chains/_rpc_retention.py``; see
# ``tests/unit/core/test_rpc_retention.py`` for the invariant that keeps it
# honest. The widening is one-directional: every legacy entry is retained.
VIB_5869_ARCHIVE_WIDENING = frozenset({"arbitrum", "bsc", "base", "optimism", "sonic", "linea"})

FROZEN_ANVIL_CHAINS = frozenset(
    {
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "avalanche",
        "bsc",
        "linea",
        "blast",
        "mantle",
        "berachain",
        "sonic",
        "monad",
        "xlayer",
        "zerog",
        "plasma",
        "hyperevm",
        "robinhood",
    }
)

FROZEN_NORMALIZATION_CHAINS = frozenset(
    {
        "ethereum",
        "arbitrum",
        "base",
        "optimism",
        "polygon",
        "avalanche",
        "bsc",
        "sonic",
        "solana",
    }
)


class TestSolanaFamilyDispatch:
    def test_solana_chain_names_exact(self) -> None:
        # Anti-widening: exactly the one Solana-family chain registered today.
        assert solana_chain_names() == frozenset({"solana"})

    def test_canonical_inputs_match_legacy_comparisons(self) -> None:
        # Byte-equivalence with the legacy ``chain == "solana"`` /
        # ``chain.lower() == "solana"`` branches for canonical inputs.
        assert is_solana_chain("solana") is True
        for name in ChainRegistry.names():
            if name != "solana":
                assert is_solana_chain(name) is False, name

    def test_documented_widening_alias_and_case(self) -> None:
        # DELIBERATE widening vs the literal comparisons: alias and cased
        # inputs now dispatch to the Solana family instead of silently
        # falling through to EVM handling.
        assert is_solana_chain("SOLANA") is True
        assert is_solana_chain(" solana ") is True
        assert is_solana_chain("sol") is True

    def test_unknown_inputs_stay_false(self) -> None:
        assert is_solana_chain("") is False
        assert is_solana_chain("gnosis") is False
        assert is_solana_chain("not-a-chain") is False


class TestRegistryDerivedEnumerations:
    def test_rate_limit_map_byte_equivalent(self) -> None:
        assert dict(rpc_rate_limit_map()) == FROZEN_CHAIN_RATE_LIMITS

    def test_rate_limit_miss_semantics_preserved(self) -> None:
        # The gateway lookup is ``CHAIN_RATE_LIMITS.get(chain, 100)`` —
        # undeclared chains must stay misses.
        assert "linea" not in rpc_rate_limit_map()
        assert "berachain" not in rpc_rate_limit_map()

    def test_archive_required_set_is_legacy_plus_documented_widening(self) -> None:
        """No longer byte-equivalent to the legacy literal — VIB-5869 widened it
        on measured evidence. Both halves are asserted so an *undocumented*
        drift still fails."""
        assert fork_archive_required_chains() == (FROZEN_ARCHIVE_RPC_REQUIRED_CHAINS | VIB_5869_ARCHIVE_WIDENING)

    def test_archive_widening_never_drops_a_legacy_entry(self) -> None:
        """Each legacy entry was added after a real production stall; the
        measurement may tighten the guard but must never loosen it."""
        assert FROZEN_ARCHIVE_RPC_REQUIRED_CHAINS <= fork_archive_required_chains()

    def test_evm_chain_names_byte_equivalent(self) -> None:
        # The legacy anvil_chains tuple enumerated exactly the registered
        # EVM chains; consumers are order-insensitive (env reads), so set
        # equality is the contract.
        assert frozenset(evm_chain_names()) == FROZEN_ANVIL_CHAINS
        assert len(evm_chain_names()) == len(FROZEN_ANVIL_CHAINS)
        assert "solana" not in evm_chain_names()

    def test_normalization_chains_documented_widening(self) -> None:
        from almanak.gateway.services._history_common import (
            SUPPORTED_NORMALIZATION_CHAINS,
        )

        # DELIBERATE widening: superset of the legacy 9-chain literal …
        assert FROZEN_NORMALIZATION_CHAINS <= SUPPORTED_NORMALIZATION_CHAINS
        # … and exactly the registry universe, nothing invented.
        assert SUPPORTED_NORMALIZATION_CHAINS == frozenset(ChainRegistry.names())

    def test_polymarket_chain_from_manifest(self) -> None:
        from almanak.framework.services.prediction_monitor import _polymarket_chain

        assert _polymarket_chain() == "polygon"

    def test_polymarket_chain_missing_manifest_fails_loud(self) -> None:
        # Manifest discovery is static, so a missing polymarket manifest is
        # a registry regression — the helper must raise, never silently
        # fall back to a hardcoded chain.
        from unittest.mock import patch

        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.framework.services.prediction_monitor import _polymarket_chain

        with patch.object(CONNECTOR_REGISTRY, "get", return_value=None):
            with pytest.raises(RuntimeError, match="polymarket connector manifest"):
                _polymarket_chain()

    def test_polymarket_chain_empty_strategy_chains_fails_loud(self) -> None:
        from types import SimpleNamespace
        from unittest.mock import patch

        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.framework.services.prediction_monitor import _polymarket_chain

        manifest = SimpleNamespace(strategy_chains=())
        with patch.object(CONNECTOR_REGISTRY, "get", return_value=manifest):
            with pytest.raises(RuntimeError, match="declares no strategy_chains"):
                _polymarket_chain()
