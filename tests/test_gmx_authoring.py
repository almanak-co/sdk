"""Named YAML author inputs fix token units, chain routing and funding before execution."""

import pytest

from almanak.caip import ChainId
from almanak.connectors.composition import connector_for
from almanak.connectors.gmx_v2.execution import preflight
from almanak.connectors.gmx_v2.fork import ForkConfig
from almanak.connectors.support import RunMode
from scripts.legacy_configuration.author import read_author
from tests.gmx_evidence import checkpoint
from tests.legacy_references import harness_directory


@pytest.mark.parametrize(
    "name,collateral,fee,capital",
    [
        ("gmx_v2_arbitrum_position", 10**17, 2 * 10**16, 5 * 10**17),
        ("gmx_v2_avalanche_position", 20 * 10**18, 10**18, 50 * 10**18),
    ],
)
def test_reference_yaml_resolves_exact_entry_fee_units_and_candidate_fork_scope(
    name, collateral, fee, capital
):
    author = read_author(harness_directory(name) / "config.yaml")
    config = author.fork_config()
    allocation = config.allocations[0]
    assert allocation.entry_collateral.amount == collateral
    assert allocation.execution_fee.amount == fee
    assert allocation.capital.amount == capital
    assert config.forks[0].chain == allocation.chain
    assert config.funds[0].assets[0].method == "wrap_native"
    support = next(
        s for s in connector_for(config).chain_support if s.chain == allocation.chain.chain_id
    )
    if allocation.chain.chain_id == ChainId("eip155:42161"):
        assert support.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
        support.require(RunMode.PRODUCTION)
    else:
        assert support.modes == (RunMode.FORK,)
        assert support.historical == ()
        with pytest.raises(ValueError, match="unsupported"):
            support.require(RunMode.PRODUCTION)
    bad = config.funds[0].model_copy(update={"account": "foreign"})
    with pytest.raises(ValueError, match="funded accounts"):
        ForkConfig.model_validate({**config.model_dump(), "funds": (bad,)})
    operator = config.operators[0]
    wrong = operator.native_balance.model_copy(update={"asset": allocation.capital.asset})
    with pytest.raises(ValueError, match="share the configured"):
        ForkConfig.model_validate(
            {
                **config.model_dump(),
                "operators": (operator.model_copy(update={"native_balance": wrong}),),
            }
        )


def test_preflight_accounts_for_fund_inventory_without_relabelling_submitter_gas():
    state = checkpoint()
    rows, empty = preflight(state.config, "fund", state.baseline)
    assert empty and rows[0]["funded"]
    assert rows[0]["required_token_base_units"] == str(5 * 10**17)
    with pytest.raises(ValueError, match="exact configured"):
        preflight(state.config, "missing", state.baseline)
    with pytest.raises(ValueError, match="target differs"):
        observation = state.baseline[0]
        target = observation.target.model_copy(update={"allocation": "foreign"})
        preflight(state.config, "fund", (observation.model_copy(update={"target": target}),))


@pytest.mark.parametrize("chain", ["eip155:42161", "eip155:43114"])
@pytest.mark.parametrize("action", ["swap_exact_input", "revoke_exchange"])
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_candidate_swap_and_cleanup_retain_native_authority_but_refuse_history(
    chain, action, profile
):
    from almanak.connectors.gmx_v2.connector import connector
    from almanak.connectors.gmx_v2.economic_data import MODEL
    from almanak.connectors.support import EconomicAction

    support = connector().support_for(ChainId(chain))
    support.require(RunMode.FORK, EconomicAction(action), profile)
    if chain == "eip155:42161":
        with pytest.raises(ValueError, match="historical model .* is unsupported"):
            support.require(RunMode.BACKTEST, EconomicAction(action), profile, model=MODEL)
    else:
        with pytest.raises(ValueError, match="backtest is unsupported"):
            support.require(RunMode.BACKTEST, EconomicAction(action), profile, model=MODEL)
    with pytest.raises(ValueError, match="historical model .* is unsupported"):
        support.require_model(MODEL)
