"""Fork gas economics require explicit operator ceilings before fixture side effects."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.configuration.fork_preparation import require_fork_policy
from almanak.configuration.parameters import gas_policies
from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.support import RunMode
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.operator_execution import PayerCeiling
from almanak.gateway.production_config import ForkGasExecution, ReplacementPolicy
from tests.test_resolution_context import revalidate

ROOT = Path(__file__).parents[1]


@pytest.fixture(params=["aave_v4_avalanche_borrowing", "aave_v4_multichain_borrowing"])
def case(request):
    name = request.param
    document = read_inputs(ROOT / "strategies" / name / "config.yaml")
    operator = read_operator_config(ROOT / "examples" / "operators" / f"{name}.fork.yaml")
    policies = gas_policies(document, RunMode.FORK)
    ceilings = tuple(
        PayerCeiling(
            accounts=policy.accounts,
            max_gas_price_wei=policy.max_gas_price_wei,
            max_gas_limit=policy.max_gas_limit,
            max_transaction_fee=policy.max_transaction_fee,
            total_budget=policy.total_budget,
            allow_replacement=policy.replacement is not None,
        )
        for policy in policies
    )
    operator = revalidate(operator, fork=revalidate(operator.fork, gas=ceilings))
    return SimpleNamespace(document=document, operator=operator, policies=policies)


def test_exact_operator_ceilings_accept_authored_fork_gas_before_fixture_setup(case):
    require_fork_policy(case.document, case.operator)
    assert case.operator.fork.check_gas(case.policies).gas == case.policies


@pytest.mark.parametrize(
    "name",
    [
        "aave_v4_avalanche_borrowing",
        "aave_v4_multichain_borrowing",
        "uniswap_v4_avalanche_lp",
        "uniswap_v4_avalanche_trading",
    ],
)
def test_shipped_operator_ceilings_preserve_the_requested_fork_economics(name):
    document = read_inputs(ROOT / "strategies" / name / "config.yaml")
    operator = read_operator_config(ROOT / "examples" / "operators" / f"{name}.fork.yaml")
    require_fork_policy(document, operator)
    assert operator.fork.check_gas(gas_policies(document, RunMode.FORK)).gas == gas_policies(
        document, RunMode.PRODUCTION
    )


@pytest.mark.parametrize("fault", ["missing", "price", "topology"])
def test_incompatible_operator_ceiling_refuses_with_author_location(case, fault):
    ceilings = list(case.operator.fork.gas)
    if fault == "missing":
        ceilings = []
    elif fault == "price":
        ceilings[0] = revalidate(
            ceilings[0], max_gas_price_wei=case.policies[0].max_gas_price_wei - 1
        )
    else:
        ceilings[0] = revalidate(ceilings[0], accounts=("unknown",))
    operator = revalidate(case.operator, fork=revalidate(case.operator.fork, gas=tuple(ceilings)))

    with pytest.raises(ConfigurationError) as error:
        require_fork_policy(case.document, operator)
    assert error.value.field == "strategy.gas"
    assert error.value.file == case.document.source.file
    assert error.value.line == case.document.source.locations["strategy.gas"]


def test_operator_must_explicitly_allow_requested_fork_replacement(case):
    policy = case.policies[0].model_copy(
        update={
            "replacement": ReplacementPolicy(
                interval_seconds=1,
                minimum_bump_bps=100,
                max_candidates=2,
                max_elapsed_seconds=2,
            )
        }
    )
    policies = (policy, *case.policies[1:])
    with pytest.raises(ValueError, match="replacement is disabled"):
        case.operator.fork.check_gas(policies)


def policy_on_chain(policy, chain, native):
    raw = policy.model_dump(mode="json")
    raw["asset"] = native
    for field in ("max_transaction_fee", "total_budget", "exit_reserve"):
        raw[field]["asset"] = native
    return type(policy).model_validate_json(json.dumps(raw))


@pytest.mark.parametrize(
    ("chain", "native"),
    [
        ("eip155:8453", "eip155:8453/slip44:60"),
        ("eip155:10", "eip155:10/slip44:60"),
    ],
)
def test_op_family_fork_gas_refuses_before_fixture_side_effects(case, monkeypatch, chain, native):
    policy = policy_on_chain(case.policies[0], chain, native)
    monkeypatch.setattr("almanak.configuration.fork_preparation.gas_policies", lambda *_: (policy,))

    with pytest.raises(ConfigurationError, match="OP fork gas admission is unavailable"):
        require_fork_policy(case.document, case.operator)
    with pytest.raises(ValueError, match="OP fork gas admission is unavailable"):
        ForkGasExecution(gas=(policy,))


@pytest.mark.parametrize(
    ("chain", "native"),
    [
        ("eip155:1", "eip155:1/slip44:60"),
        ("eip155:43114", "eip155:43114/slip44:9005"),
    ],
)
def test_bounded_ethereum_and_avalanche_fork_gas_remains_supported(case, chain, native):
    policy = policy_on_chain(case.policies[0], chain, native)
    assert ForkGasExecution(gas=(policy,)).gas == (policy,)


def test_duplicate_authored_account_coverage_refuses_before_fixture_side_effects(case):
    policy = case.policies[0]
    with pytest.raises(ValueError, match="each account requires exactly one gas policy"):
        case.operator.fork.check_gas((policy, policy))


@pytest.mark.parametrize("name", ["aave_v3_base_lending", "aave_v3_optimism_lending"])
def test_gasless_op_family_fork_operator_remains_supported(name):
    operator = read_operator_config(ROOT / "examples" / "operators" / f"{name}.fork.yaml")
    assert operator.fork.check_gas(()) is None
    assert "gas" not in operator.model_dump(mode="json")["fork"]
