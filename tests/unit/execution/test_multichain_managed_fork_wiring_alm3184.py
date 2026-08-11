"""ALM-3184 positive managed-fork signal in the legacy multichain compile lane."""

from __future__ import annotations

import pytest

from almanak.core.rpc_network import Network
from almanak.framework.execution.config import MultiChainRuntimeConfig
from almanak.framework.execution.multichain import MultiChainOrchestrator


@pytest.mark.parametrize(
    ("network", "expected"),
    [
        (Network.ANVIL, True),
        ("anvil", True),
        (Network.MAINNET, False),
        ("mainnet", False),
        (None, False),
        ("anvil-ish", False),
    ],
)
def test_multichain_compiler_threads_only_explicit_anvil(network, expected):
    config = MultiChainRuntimeConfig(
        chains=["arbitrum"],
        protocols={"arbitrum": ["uniswap_v3"]},
        private_key="0x" + "11" * 32,
        network=Network.ANVIL,
    )
    # Exercise the signal's fail-closed handling of legacy/untyped values while
    # retaining the real production config schema at the orchestrator boundary.
    config.network = network
    orchestrator = MultiChainOrchestrator(config=config)

    compiler = orchestrator._get_compiler("arbitrum")

    assert compiler._config.managed_fork is expected
