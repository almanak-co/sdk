"""Tests for is_multi_chain_strategy() detection.

Multi-chain mode is determined by:
1. Config's "chains" list with >1 entry (highest priority)
2. Config dataclass with a "chains" field defaulting to >1 chain
3. Legacy SUPPORTED_CHAINS class attribute

The decorator's supported_chains is NOT used — it's portability metadata.
"""

import dataclasses
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from almanak.framework.cli.run import is_multi_chain_strategy, resolve_identity_chain


class TestIsMultiChainStrategy:
    """Tests for is_multi_chain_strategy()."""

    def test_config_chains_list_triggers_multi_chain(self):
        """Config with "chains": ["base", "arbitrum"] enables multi-chain mode."""

        class FakeStrategy:
            pass

        config = {"chains": ["base", "arbitrum"]}
        assert is_multi_chain_strategy(FakeStrategy, config=config) is True

    def test_config_single_chain_list_not_multi(self):
        """Config with "chains": ["arbitrum"] is NOT multi-chain."""

        class FakeStrategy:
            pass

        config = {"chains": ["arbitrum"]}
        assert is_multi_chain_strategy(FakeStrategy, config=config) is False

    def test_config_no_chains_not_multi(self):
        """Config without "chains" key is NOT multi-chain."""

        class FakeStrategy:
            pass

        config = {"chain": "arbitrum"}
        assert is_multi_chain_strategy(FakeStrategy, config=config) is False

    def test_decorator_supported_chains_does_not_trigger_multi(self):
        """Decorator supported_chains is portability metadata, not multi-chain signal."""

        class FakeStrategy:
            STRATEGY_METADATA = SimpleNamespace(
                supported_chains=["ethereum", "arbitrum", "optimism", "polygon", "base"]
            )

        # No config chains — portable strategy, NOT multi-chain
        assert is_multi_chain_strategy(FakeStrategy) is False
        assert is_multi_chain_strategy(FakeStrategy, config={"chain": "arbitrum"}) is False

    def test_dataclass_config_chains_default_triggers_multi(self):
        """Strategy with dataclass config having chains default >1 is multi-chain."""
        from almanak.framework.strategies import IntentStrategy

        @dataclass
        class MultiChainConfig:
            primary_chain: str = "arbitrum"
            chains: list[str] = field(default_factory=lambda: ["arbitrum", "optimism", "base"])

        class FakeStrategy(IntentStrategy[MultiChainConfig]):
            pass

        # No chains in config dict, but dataclass default has 3 chains
        assert is_multi_chain_strategy(FakeStrategy, config={"deployment_id": "test"}) is True

    def test_dataclass_config_single_chain_not_multi(self):
        """Strategy with dataclass config having chains default of 1 is NOT multi-chain."""
        from almanak.framework.strategies import IntentStrategy

        @dataclass
        class SingleChainConfig:
            chain: str = "base"
            chains: list[str] = field(default_factory=lambda: ["base"])

        class FakeStrategy(IntentStrategy[SingleChainConfig]):
            pass

        assert is_multi_chain_strategy(FakeStrategy, config={"deployment_id": "test"}) is False

    def test_legacy_supported_chains_multi(self):
        """Strategy with legacy SUPPORTED_CHAINS = ["base", "arbitrum"] is multi-chain."""

        class FakeStrategy:
            SUPPORTED_CHAINS = ["base", "arbitrum"]

        assert is_multi_chain_strategy(FakeStrategy) is True

    def test_legacy_supported_chains_single(self):
        """Strategy with legacy SUPPORTED_CHAINS = ["arbitrum"] is NOT multi-chain."""

        class FakeStrategy:
            SUPPORTED_CHAINS = ["arbitrum"]

        assert is_multi_chain_strategy(FakeStrategy) is False

    def test_no_chains_at_all(self):
        """Strategy with neither attribute nor config is NOT multi-chain."""

        class FakeStrategy:
            pass

        assert is_multi_chain_strategy(FakeStrategy) is False

    def test_config_chains_overrides_legacy(self):
        """Config chains takes precedence over legacy SUPPORTED_CHAINS."""

        class FakeStrategy:
            SUPPORTED_CHAINS = ["arbitrum"]  # Single-chain legacy

        config = {"chains": ["base", "arbitrum"]}
        assert is_multi_chain_strategy(FakeStrategy, config=config) is True

    def test_config_chains_non_list_ignored(self):
        """Non-list 'chains' values in config are ignored."""

        class FakeStrategy:
            pass

        assert is_multi_chain_strategy(FakeStrategy, config={"chains": "arbitrum"}) is False
        assert is_multi_chain_strategy(FakeStrategy, config={"chains": 42}) is False


class TestResolveIdentityChain:
    """Codex review of PR #3838: a scalar `chain` is the WRONG identity input
    for a multi-chain strategy — `_run_setup.py:_resolve_identity()` hashes
    the sorted comma-joined `config["chains"]` list instead. This is the
    single canonical helper both the runner's own boot-time resolution and
    any independent recomputation (teardown execute's cold-state
    self-resolution) must share, so they can never diverge."""

    def test_single_chain_strategy_returns_the_scalar_chain_unchanged(self):
        class FakeStrategy:
            pass

        assert resolve_identity_chain(strategy_class=FakeStrategy, config={}, chain="base") == "base"

    def test_multi_chain_via_config_returns_sorted_lowercased_joined_chains(self):
        class FakeStrategy:
            pass

        config = {"chains": ["Base", "Arbitrum"]}
        # Sorted, lower-cased — matching _resolve_identity()'s exact contract.
        assert resolve_identity_chain(strategy_class=FakeStrategy, config=config, chain="base") == "arbitrum,base"

    def test_multi_chain_via_config_is_order_independent(self):
        class FakeStrategy:
            pass

        forward = resolve_identity_chain(strategy_class=FakeStrategy, config={"chains": ["base", "arbitrum"]}, chain="")
        reverse = resolve_identity_chain(strategy_class=FakeStrategy, config={"chains": ["arbitrum", "base"]}, chain="")
        assert forward == reverse == "arbitrum,base"

    def test_multi_chain_via_legacy_supported_chains_when_config_has_no_chains_list(self):
        """A multi-chain strategy declared via the legacy SUPPORTED_CHAINS
        attribute (no config["chains"]) still resolves via its declared
        chains, not the scalar `chain` passed in."""

        class FakeStrategy:
            SUPPORTED_CHAINS = ["base", "arbitrum"]

        assert resolve_identity_chain(strategy_class=FakeStrategy, config={}, chain="base") == "arbitrum,base"

    def test_matches_resolve_deployment_id_hash_exactly(self):
        """End-to-end: the identity chain this function produces must hash
        to the SAME deployment_id the runner's own boot-time resolution
        would produce for the same strategy+config — reproduces the exact
        hashes reported in review (`deployment:4c0c744c61dc` for the WRONG
        single-chain hash, `deployment:d59e81b06d68` for the correct one)."""
        from almanak.framework.runner.identity import resolve_deployment_id

        class FakeStrategy:
            pass

        config = {"chains": ["base", "arbitrum"]}
        identity_chain = resolve_identity_chain(strategy_class=FakeStrategy, config=config, chain="base")

        assert resolve_deployment_id(wallet_address="0xabc", chain=identity_chain) == "deployment:d59e81b06d68"
        assert resolve_deployment_id(wallet_address="0xabc", chain="base") == "deployment:4c0c744c61dc"
