"""A committed Uniswap LP approval remains a cleanup obligation after process replacement."""

from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composition import connector_for
from almanak.gateway.operator_config import read_operator_config
from tests.fork.support import exercise_approval_cleanup
from tests.fork.test_aave_v3_base_eoa import eoa_configuration
from tests.fork.test_pancakeswap_v3_eoa import qualified_fund_indices as pancake_lp_fund_indices
from tests.fork.test_uniswap_v3_eoa import lp_fund_indices as v3_lp_fund_indices
from tests.fork.test_uniswap_v4_eoa import lp_fund_indices as v4_lp_fund_indices
from tests.qa.catalog import uniswap_lp_cleanup_references, uniswap_lp_family
from tests.strategy_fork.adapter import fixture_plan_for, prepare_admitted_fork, resolve_reference
from tests.strategy_fork.harness import case_fork_config, discover, resolve_config, retain
from tests.strategy_fork.supervised import eoa_inputs, revalidated
from tests.strategy_fork.v3_pool_assertions import assert_payer

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]
# The first ERC-20 approval each family commits before any swap or mint, and the
# zero-amount revocation its closure must deliver for that same allowance.
APPROVALS = {
    "pancakeswap_v3": ("approve", "approve"),
    "uniswap_v3": ("approve", "approve"),
    "uniswap_v4": ("approve_erc20", "revoke_erc20"),
}
PHASES = (
    "approval_committed_before_outcome_delivery",
    "replaced_runtime_and_gateway_revoked_without_trading",
)


def eoa_lp_configuration(reference, config):
    """Plain EOA funds at the exact indices the family's origin qualifications use."""
    if uniswap_lp_family(reference) == "pancakeswap_v3":
        return pancake_lp_fund_indices(eoa_configuration(config), lp=True)
    indices = (
        v4_lp_fund_indices if uniswap_lp_family(reference) == "uniswap_v4" else v3_lp_fund_indices
    )
    return indices(eoa_configuration(config))


@pytest.mark.parametrize("reference", uniswap_lp_cleanup_references())
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_uniswap_lp_approval_cleanup_after_committed_effect(request, tmp_path, reference, profile):
    case = next(
        value
        for value in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if value.name == reference
    )
    env = request.config.getoption("--fork-env").resolve()
    config, prepare = cleanup_configuration(case, profile, env, tmp_path)
    artifact = ROOT / "artifacts/strategy-fork" / f"{reference}-cleanup-{profile}-{uuid4().hex}"
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": reference,
        "account_profile": profile,
        "scenario": "approval_commit_then_teardown",
        "status": "failed",
        "phases": [],
    }
    try:
        exercise(case, config, env, temporary, record, prepare=prepare)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)


def cleanup_configuration(case, profile, env, directory):
    if not read_inputs(case.config).inputs.strategy.gas:
        config = case_fork_config(case, env, directory / "resolution")
        if profile == "eoa":
            config = eoa_lp_configuration(case.name, config)
        return resolve_config(config, env, {}), None
    source = read_operator_config(ROOT / "examples/operators" / f"{case.name}.fork.yaml")
    operator = revalidated(source, fork=eoa_inputs(source.fork)) if profile == "eoa" else source
    operator_path = directory / f"{profile}.operator.json"
    operator_path.write_text(operator.model_dump_json(indent=2))
    progress = resolve_reference(case.config, operator_path, env, directory / "resolution")
    plan = fixture_plan_for(case.config, operator_path, progress)

    def prepare(nodes, target):
        return prepare_admitted_fork(
            case.config, case.strategy, operator_path, progress, plan, nodes, target
        )

    return plan, prepare


def exercise(case, config, env, temporary, record, *, prepare=None):
    approval, _ = APPROVALS[uniswap_lp_family(case.name)]
    exercise_approval_cleanup(
        case,
        config,
        env,
        temporary,
        record,
        operation=approval,
        assert_committed=assert_approval_committed,
        assert_cleanup=assert_cleanup,
        prepare=prepare,
    )


def family_of(state):
    """The connector family of a retained checkpoint, from its own configuration."""
    return uniswap_lp_family(state.config.connector)


def assert_approval_committed(state):
    approval, _ = APPROVALS[family_of(state)]
    assert (len(state.outcomes), state.delivered, state.pending) == (1, 0, None)
    assert_approval_outcome(state.outcomes[0], approval)
    assert all(owned.position is None for owned in state.owned)


def assert_approval_outcome(outcome, approval):
    assert outcome.success and outcome.operation.kind == approval
    assert outcome.operation.amount > 0 and outcome.after.needs_cleanup


def assert_closed_effects(before, closed):
    approval, revocation = APPROVALS[family_of(closed)]
    assert (closed.mode, closed.pending) == ("closed", None)
    assert closed.delivered == len(closed.outcomes) == 2
    assert closed.outcomes[0] == before.outcomes[0]
    assert_revocation(before.outcomes[0].operation, closed.outcomes[1], revocation)
    # No swap, mint, collect or burn was ever submitted: only the approval pair exists.
    assert {o.operation.kind for o in closed.outcomes} == {approval, revocation}


def assert_revocation(approved, revoked, revocation):
    assert revoked.success and not revoked.after.needs_cleanup
    assert (revoked.operation.kind, revoked.operation.amount) == (revocation, 0)
    assert (revoked.operation.target, revoked.operation.asset) == (approved.target, approved.asset)


def assert_allocation_cleanup(owned, initial, final):
    assert owned.position is None and not owned.entry_complete and not final.needs_cleanup
    assert (
        (owned.inventory0, owned.inventory1)
        == (initial.wallet0, initial.wallet1)
        == (final.wallet0, final.wallet1)
    )


def assert_observed_clear(observed, owned, family):
    assert (observed.wallet0, observed.wallet1) == (owned.inventory0, owned.inventory1)
    assert not observed.needs_cleanup
    assert remaining_position(observed, family) == (None, 0)


def remaining_position(observed, family):
    """The position the venue still reports and its liquidity, which only V4 observes."""
    if family == "uniswap_v4":
        return observed.owned.position, observed.position_liquidity
    return observed.position, 0


def assert_cleanup(node, before, closed):
    from almanak.gateway.ethereum import EthereumReader

    assert_closed_effects(before, closed)
    snapshot = connector_for(closed.config).execution.snapshot
    with EthereumReader(closed.owned[0].target.fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        backend = SimpleNamespace(config=closed.config, reader=reader)
        for owned, initial, final in zip(
            closed.owned, closed.baseline, closed.closure, strict=True
        ):
            assert_allocation_cleanup(owned, initial, final)
            observed = snapshot(backend, owned.target.allocation, block, owned, quotes=False)
            assert_observed_clear(observed, owned, family_of(closed))
            effects = [o for o in closed.outcomes if o.operation.target == owned.target]
            assert_payer(closed, owned.target, initial, final, effects)
