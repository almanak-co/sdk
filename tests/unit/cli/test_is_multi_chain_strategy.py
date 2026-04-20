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

from almanak.framework.cli.run import is_multi_chain_strategy


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
        assert is_multi_chain_strategy(FakeStrategy, config={"strategy_id": "test"}) is True

    def test_dataclass_config_single_chain_not_multi(self):
        """Strategy with dataclass config having chains default of 1 is NOT multi-chain."""
        from almanak.framework.strategies import IntentStrategy

        @dataclass
        class SingleChainConfig:
            chain: str = "base"
            chains: list[str] = field(default_factory=lambda: ["base"])

        class FakeStrategy(IntentStrategy[SingleChainConfig]):
            pass

        assert is_multi_chain_strategy(FakeStrategy, config={"strategy_id": "test"}) is False

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
