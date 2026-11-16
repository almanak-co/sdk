"""A committed approval remains a cleanup obligation after process replacement."""

from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.support import exercise_approval_cleanup
from tests.fork.test_aave_v3_base_eoa import eoa_configuration
from tests.strategy_fork.curve_assertions import assert_gas_cost, assert_inventory, assert_shares
from tests.strategy_fork.harness import case_fork_config, discover, resolve_config, retain

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize(
    "chain,family",
    [
        ("ethereum", "stable"),
        ("ethereum", "twocrypto"),
        ("ethereum", "tricrypto"),
        ("base", "stable"),
        ("base", "twocrypto"),
        ("base", "tricrypto"),
        ("arbitrum", "stable"),
        ("arbitrum", "twocrypto"),
        ("arbitrum", "tricrypto"),
        ("polygon", "stable"),
        ("polygon", "twocrypto"),
        ("polygon", "tricrypto"),
        ("bnb", "stable"),
        ("bnb", "twocrypto"),
        ("avalanche", "stable"),
        ("optimism", "stable"),
        ("optimism", "twocrypto"),
        ("optimism", "tricrypto"),
        ("robinhood", "stable"),
        ("robinhood", "twocrypto"),
        ("robinhood", "tricrypto"),
    ],
)
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_curve_approval_cleanup_after_committed_effect(request, tmp_path, chain, family, profile):
    name = f"curve_{chain}_{family}_lp"
    case = next(
        value
        for value in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if value.name == name
    )
    env = request.config.getoption("--fork-env").resolve()
    config = case_fork_config(case, env, tmp_path / "resolution")
    if profile == "eoa":
        config = eoa_configuration(config)
    config = resolve_config(config, env, {})
    artifact = ROOT / "artifacts/strategy-fork" / (name + "-cleanup-" + profile + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": name,
        "account_profile": profile,
        "scenario": "approval_commit_then_teardown",
        "status": "failed",
        "phases": [],
    }
    try:
        exercise(case, config, env, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)


def exercise(case, config, env, temporary, record):
    exercise_approval_cleanup(
        case,
        config,
        env,
        temporary,
        record,
        operation="approve",
        assert_committed=assert_approval_committed,
        assert_cleanup=assert_cleanup,
    )


def assert_approval_committed(state):
    assert len(state.outcomes) == 1 and state.delivered == 0
    outcome = state.outcomes[0]
    assert outcome.success and outcome.operation.kind == "approve"
    assert outcome.operation.amount > 0 and outcome.after.needs_cleanup
    assert state.pending is None


def assert_cleanup(node, before, closed):
    from almanak.gateway.ethereum import EthereumReader

    assert_closed_effects(before, closed)
    with EthereumReader(closed.owned[0].target.fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for owned, initial, final in zip(
            closed.owned, closed.baseline, closed.closure, strict=True
        ):
            assert_allocation_cleanup(owned, initial, final)
            assert_inventory(reader, block, owned)
            assert_shares(reader, block, owned)
            events = [o for o in closed.outcomes if o.execution.submitter == initial.payer]
            assert_gas_cost(events, initial, final)


def assert_closed_effects(before, closed):
    assert closed.pending is None and closed.delivered == len(closed.outcomes) == 2
    assert closed.outcomes[0] == before.outcomes[0]
    revoked = closed.outcomes[1]
    assert revoked.success and revoked.operation.kind == "approve"
    assert revoked.operation.amount == 0
    assert revoked.operation.target == before.outcomes[0].operation.target
    assert not revoked.after.needs_cleanup


def assert_allocation_cleanup(owned, initial, final):
    assert owned.position is None and not final.needs_cleanup
    assert owned.inventory == initial.wallets == final.wallets
