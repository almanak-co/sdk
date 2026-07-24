"""Unit tests for ``SimulationConfig.can_simulate_chain``.

Covers every branch of the chain-capability check:

- alias normalization through ``resolve_chain_name`` ("bnb" -> "bsc")
- ValueError fallback to plain ``.lower()`` for unknown chain strings
- Tenderly-supported vs Alchemy-supported vs unsupported chains
- unconfigured simulators always return False
"""

from __future__ import annotations

from almanak.framework.execution.simulator.config import (
    ALCHEMY_SUPPORTED_CHAINS,
    TENDERLY_SUPPORTED_CHAINS,
    SimulationConfig,
)


def _tenderly_config() -> SimulationConfig:
    return SimulationConfig(
        enabled=True,
        tenderly_account="acct",
        tenderly_project="proj",
        tenderly_access_key="key",
    )


def _alchemy_config() -> SimulationConfig:
    return SimulationConfig(enabled=True, alchemy_api_key="alchemy-key")


class TestCanSimulateChain:
    def test_tenderly_supported_chain(self) -> None:
        assert "arbitrum" in TENDERLY_SUPPORTED_CHAINS  # registry guard
        assert _tenderly_config().can_simulate_chain("arbitrum") is True

    def test_alias_resolves_before_lookup(self) -> None:
        # "bnb" is an alias for "bsc"; only the canonical name is in the set.
        assert "bnb" not in TENDERLY_SUPPORTED_CHAINS
        assert "bsc" in TENDERLY_SUPPORTED_CHAINS
        assert _tenderly_config().can_simulate_chain("bnb") is True

    def test_alchemy_supported_chain(self) -> None:
        assert "base" in ALCHEMY_SUPPORTED_CHAINS
        assert _alchemy_config().can_simulate_chain("base") is True

    def test_alchemy_config_does_not_cover_tenderly_only_chain(self) -> None:
        # bsc is Tenderly-simulatable but not in the Alchemy set.
        assert "bsc" not in ALCHEMY_SUPPORTED_CHAINS
        assert _alchemy_config().can_simulate_chain("bsc") is False

    def test_unknown_chain_falls_back_to_lower_and_fails(self) -> None:
        # Not a registry chain: resolve_chain_name raises, .lower() fallback used.
        assert _tenderly_config().can_simulate_chain("NotAChain") is False

    def test_unconfigured_simulators_return_false(self) -> None:
        config = SimulationConfig(enabled=True)
        assert config.can_simulate_chain("arbitrum") is False

    def test_non_evm_chain_unsupported(self) -> None:
        config = SimulationConfig(
            enabled=True,
            tenderly_account="acct",
            tenderly_project="proj",
            tenderly_access_key="key",
            alchemy_api_key="alchemy-key",
        )
        # Solana resolves in the registry but simulation is EVM-only.
        assert config.can_simulate_chain("solana") is False
