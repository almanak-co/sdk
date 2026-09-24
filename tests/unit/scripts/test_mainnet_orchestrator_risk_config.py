"""The live-funds mainnet orchestrator must carry its chain's gas ceilings.

`ExecutionOrchestrator` falls back to `TransactionRiskConfig.default()` when the
caller passes none. That default leaves `max_gas_cost_native` at 0.0 -- meaning
no absolute per-transaction gas ceiling at all -- on a lane that spends real
money. The runner must pass `for_chain(chain)` so the descriptor's
`cost_cap_native` and `price_cap_gwei` actually bind.
"""

from unittest.mock import MagicMock

import pytest

from almanak.core.chains import ChainRegistry
from almanak.framework.execution.orchestrator import TransactionRiskConfig
from qa_lab.mainnet_intent_recipe import RECIPES
from qa_lab.run_mainnet_intent import _orchestrator

# Chains that actually carry live funds in the mainnet lane, taken from the
# recipe table so a newly added chain is covered without editing this file.
LANE_CHAINS = sorted({recipe.chain for recipe in RECIPES.values()})

# Lane chains whose descriptor declares no gas ceilings, leaving `for_chain` with
# only the generic fallback to hand back. Wiring the runner cannot cap these --
# the gap is the missing GasProfile. Tightening a descriptor must fail here and be
# removed from this set, and adding an uncapped chain to the lane must fail too.
DESCRIPTOR_HAS_NO_CAPS = {"robinhood"}


def _build(chain: str):
    return _orchestrator(
        private_key="0x" + "11" * 32,
        rpc_url="http://127.0.0.1:8545",
        chain=chain,
        gateway_client=MagicMock(),
    )


@pytest.mark.parametrize("chain", LANE_CHAINS)
def test_orchestrator_carries_chain_specific_risk_config(chain):
    expected = TransactionRiskConfig.for_chain(chain)
    actual = _build(chain).tx_risk_config

    assert actual.max_gas_price_gwei == expected.max_gas_price_gwei
    assert actual.max_gas_cost_native == expected.max_gas_cost_native
    assert actual.block_contract_deployment is True


@pytest.mark.parametrize("chain", sorted(set(LANE_CHAINS) - DESCRIPTOR_HAS_NO_CAPS))
def test_capped_lane_chains_have_a_real_native_gas_ceiling(chain):
    """The property that matters: a bounded absolute cost per transaction.

    `default()` satisfies every gwei-cap assertion above on some chains while
    leaving this at 0.0, so this is the assertion that a missing
    `tx_risk_config` actually fails.
    """
    config = _build(chain).tx_risk_config

    assert config.max_gas_cost_native > 0, (
        f"{chain} runs with no absolute per-tx gas ceiling; the runner is not passing TransactionRiskConfig.for_chain()"
    )


@pytest.mark.parametrize("chain", sorted(DESCRIPTOR_HAS_NO_CAPS))
def test_uncapped_lane_chains_are_uncapped_because_the_descriptor_is_empty(chain):
    """Pin the reason, so a descriptor fix is noticed instead of staying dormant."""
    descriptor = ChainRegistry.try_resolve(chain)

    assert descriptor is not None, f"{chain} is in the recipe table but has no descriptor"
    assert descriptor.gas.cost_cap_native is None and descriptor.gas.price_cap_gwei is None, (
        f"{chain} now declares gas ceilings -- drop it from DESCRIPTOR_HAS_NO_CAPS so the lane asserts they bind"
    )
