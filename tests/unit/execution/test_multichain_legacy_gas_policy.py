"""Legacy execution refuses cost limits it cannot enforce before creating a signer."""

from unittest.mock import patch

import pytest

from almanak.core.rpc_network import Network
from almanak.framework.execution.config import ConfigurationError, MultiChainRuntimeConfig
from almanak.framework.execution.multichain import MultiChainOrchestrator


def _config():
    return MultiChainRuntimeConfig(
        chains=["arbitrum"],
        protocols={"arbitrum": ["uniswap_v3"]},
        private_key="0x" + "11" * 32,
        network=Network.ANVIL,
    )


@pytest.mark.parametrize("field", ["max_gas_cost_native", "max_gas_cost_usd"])
@pytest.mark.parametrize("value", [0.01, -1, float("nan"), float("inf"), None, "malformed"])
def test_legacy_cost_policy_refuses_before_executor_creation(field, value):
    config = _config()
    # Runtime configuration is mutable; validate even if altered after construction.
    setattr(config, field, value)
    with patch("almanak.framework.execution.multichain.ChainExecutor") as executor:
        with pytest.raises(ConfigurationError) as error:
            MultiChainOrchestrator.from_config(config)
        executor.assert_not_called()
    assert error.value.field == field
    if value == 0.01:
        assert "from_gateway" in str(error.value)


def test_legacy_zero_cost_limits_preserve_existing_lazy_construction():
    with patch("almanak.framework.execution.multichain.ChainExecutor") as executor:
        orchestrator = MultiChainOrchestrator.from_config(_config())
        executor.assert_not_called()
    assert orchestrator.chains == ["arbitrum"]
