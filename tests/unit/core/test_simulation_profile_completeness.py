"""VIB-4851 Phase 2: pin the registry-derived simulation-support maps.

Phase 2 folded the per-chain Tenderly / Alchemy SIMULATION-API support map
onto ``ChainDescriptor.simulation`` (``SimulationProfile``) and rewired
``almanak/framework/execution/simulator/config.py`` to DERIVE its four maps
(``TENDERLY_NETWORK_IDS``, ``ALCHEMY_NETWORKS``, ``TENDERLY_SUPPORTED_CHAINS``,
``ALCHEMY_SUPPORTED_CHAINS``) from the chain registry instead of hardcoding
chain-name literals.

These tests are a TRUE characterization: the expected values are hand-keyed
HARD-CODED literals — NOT derived from the same registry the production code
reads. If a future descriptor edit mistypes a ``chain_id`` (the historical
1648-vs-9745 drift), drops one of the 14 Tenderly chains, or accidentally
marks a non-supported chain (xlayer / zerog / solana), one of these assertions
fails loudly at merge time.

The Tenderly network-id VALUE is asserted to equal ``str(chain_id)`` — the
value is never stored on the descriptor, so this also pins the derivation rule.
"""

from __future__ import annotations

from types import MappingProxyType

from almanak.core.chains import ChainRegistry
from almanak.framework.execution.simulator.config import (
    ALCHEMY_NETWORKS,
    ALCHEMY_SUPPORTED_CHAINS,
    TENDERLY_NETWORK_IDS,
    TENDERLY_SUPPORTED_CHAINS,
)

# =============================================================================
# Frozen historical snapshots — the exact pre-fold literals that lived in
# ``simulator/config.py`` before VIB-4851 Phase 2. Hand-keyed so a regression
# diff against ``main`` is obvious, and so the assertions cannot pass just
# because the fold reproduced its own (possibly wrong) source.
# =============================================================================

EXPECTED_TENDERLY_NETWORK_IDS: dict[str, str] = {
    "ethereum": "1",
    "arbitrum": "42161",
    "optimism": "10",
    "polygon": "137",
    "base": "8453",
    "avalanche": "43114",
    "bsc": "56",
    "linea": "59144",
    "plasma": "9745",
    "sonic": "146",
    "blast": "81457",
    "mantle": "5000",
    "berachain": "80094",
    "monad": "143",
}

EXPECTED_ALCHEMY_NETWORKS: dict[str, str] = {
    "ethereum": "eth-mainnet",
    "arbitrum": "arb-mainnet",
    "optimism": "opt-mainnet",
    "base": "base-mainnet",
}

# Chains that simulation must NOT cover — guards against an accidental
# ``simulation=SimulationProfile(tenderly_supported=True)`` slipping onto a
# chain the simulators do not support.
UNSUPPORTED_CHAINS: frozenset[str] = frozenset({"xlayer", "zerog", "solana"})


class TestTenderlyNetworkIds:
    """``TENDERLY_NETWORK_IDS`` reproduces the pre-fold 14-entry map exactly."""

    def test_exact_map_equality(self) -> None:
        # ``dict(...)`` unwraps the MappingProxyType for a clean equality diff.
        assert dict(TENDERLY_NETWORK_IDS) == EXPECTED_TENDERLY_NETWORK_IDS

    def test_plasma_chain_id_pinned(self) -> None:
        # The historical 1648-vs-9745 drift bug: plasma's network id is the
        # real EIP-155 chain id 9745, never the legacy 1648.
        assert TENDERLY_NETWORK_IDS["plasma"] == "9745"

    def test_is_read_only_mapping(self) -> None:
        # Derived views must be immutable so the registry stays the single
        # source of truth.
        assert isinstance(TENDERLY_NETWORK_IDS, MappingProxyType)

    def test_value_equals_str_chain_id_for_every_supported_chain(self) -> None:
        # The Tenderly network-id VALUE is never stored on the descriptor; it
        # is always ``str(descriptor.chain_id)``. Pin that derivation rule.
        for descriptor in ChainRegistry.all():
            if descriptor.simulation.tenderly_supported:
                assert TENDERLY_NETWORK_IDS[descriptor.name] == str(descriptor.chain_id), (
                    f"{descriptor.name}: Tenderly network id must equal str(chain_id)={descriptor.chain_id}"
                )


class TestAlchemyNetworks:
    """``ALCHEMY_NETWORKS`` reproduces the pre-fold 4-entry map exactly."""

    def test_exact_map_equality(self) -> None:
        assert dict(ALCHEMY_NETWORKS) == EXPECTED_ALCHEMY_NETWORKS

    def test_is_read_only_mapping(self) -> None:
        assert isinstance(ALCHEMY_NETWORKS, MappingProxyType)

    def test_value_matches_descriptor_profile(self) -> None:
        for descriptor in ChainRegistry.all():
            network = descriptor.simulation.alchemy_network
            if network is not None:
                assert ALCHEMY_NETWORKS[descriptor.name] == network


class TestSupportedChainSets:
    """The supported-chain frozensets are the keys of their network maps."""

    def test_tenderly_supported_has_14_members(self) -> None:
        assert len(TENDERLY_SUPPORTED_CHAINS) == 14

    def test_tenderly_supported_equals_expected_keys(self) -> None:
        assert TENDERLY_SUPPORTED_CHAINS == frozenset(EXPECTED_TENDERLY_NETWORK_IDS)

    def test_alchemy_supported_equals_expected_keys(self) -> None:
        assert ALCHEMY_SUPPORTED_CHAINS == frozenset(EXPECTED_ALCHEMY_NETWORKS)

    def test_alchemy_is_subset_of_tenderly(self) -> None:
        # Every Alchemy-simulation chain is also a Tenderly-simulation chain.
        assert ALCHEMY_SUPPORTED_CHAINS <= TENDERLY_SUPPORTED_CHAINS

    def test_unsupported_chains_excluded_from_tenderly(self) -> None:
        for chain in UNSUPPORTED_CHAINS:
            assert chain not in TENDERLY_SUPPORTED_CHAINS

    def test_unsupported_chains_excluded_from_alchemy(self) -> None:
        for chain in UNSUPPORTED_CHAINS:
            assert chain not in ALCHEMY_SUPPORTED_CHAINS


class TestDescriptorProfileConsistency:
    """The registry descriptors agree with the derived config maps."""

    def test_tenderly_supported_descriptors_match_map_keys(self) -> None:
        supported = {d.name for d in ChainRegistry.all() if d.simulation.tenderly_supported}
        assert supported == set(TENDERLY_NETWORK_IDS)

    def test_alchemy_network_descriptors_match_map_keys(self) -> None:
        with_alchemy = {d.name for d in ChainRegistry.all() if d.simulation.alchemy_network is not None}
        assert with_alchemy == set(ALCHEMY_NETWORKS)
