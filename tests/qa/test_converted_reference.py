"""The converted reference keeps its QA inputs, providers and fork routing honest.

Every check here is offline: derived operator files must load and compile, and the
supervised driver is recorded rather than started.
"""

import pytest

from almanak.caip import ChainId
from almanak.configuration.backtest_inputs import read_backtest_inputs
from almanak.configuration.unresolved import read_inputs
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.operator_config import read_operator_config
from tests.qa.catalog import ROOT, catalog, converted
from tests.qa.inputs import fresh_targets, require_providers, resolved_author, resolved_history
from tests.qa.operators import case_operator, require_fixture

ANCHOR = "0xebd2f4a6612e094a8db8d1ab770970f7060bdaf3a18a49e4274f8935d5348ea0"
CHAIN = ChainId("eip155:1")
ORIGINS = {
    CHAIN: {
        "chain": {"chain_id": "eip155:1", "environment": "mainnet"},
        "number": 25999999,
        "hash": "0x" + "ab" * 32,
        "timestamp": 1787000000,
    }
}
SECOND_PUBLIC = "ethereum_aave_v4_borrow_and_trade"
CASES = catalog()


def case_for(case_id):
    return next(c for c in CASES if c.id == case_id)


def expected_operators(profile, operator_mode):
    if profile == "eoa":
        return set()
    return {"operator_0", "operator_1"} if operator_mode == "distinct" else {"operator_shared"}


@pytest.mark.parametrize("policy", ["pinned", "fresh"])
@pytest.mark.parametrize(
    "profile,operator_mode",
    [("safe_roles", "configured"), ("eoa", "configured"), ("safe_roles", "distinct")],
)
def test_derived_case_operators_load_and_compile(tmp_path, policy, profile, operator_mode):
    case = case_for(f"fork/{policy}/aave_v4_weth_supply/{profile}/{operator_mode}")
    operator = read_operator_config(case_operator(case, tmp_path, ORIGINS))
    fixture = operator.fork
    anchor = fixture.anchors[0]
    expected = (
        (ORIGINS[CHAIN]["number"], ORIGINS[CHAIN]["hash"])
        if policy == "fresh"
        else (25908472, ANCHOR)
    )
    assert (anchor.height, str(anchor.reference)) == expected
    assert {fund.kind for fund in fixture.funds} == {profile}
    assert {o.name for o in fixture.operators} == expected_operators(profile, operator_mode)
    assert {fixture.native_balance(f).amount for f in fixture.funds} == {5000000000000000000}
    plan = require_fixture(case, tmp_path, operator)
    assert len(plan.funds) == 2 and plan.forks[0].block.number == expected[0]


def test_backtest_case_operator_keeps_the_read_only_provider_binding(tmp_path):
    case = case_for("backtest/pinned/aave_v4_weth_supply/economic")
    operator = read_operator_config(case_operator(case, tmp_path, {}))
    assert operator.fork is None and operator.production is None
    assert not operator.accounts.bindings
    assert [n.provider.api_key_env for n in operator.networks] == ["ALCHEMY_API_KEY"]


def test_fresh_targets_and_providers_cover_converted_and_harness_references(tmp_path):
    fork = [c for c in CASES if c.lane == "fork" and c.input_policy == "fresh"]
    selected = [c for c in fork if c.reference == "aave_v4_weth_supply"]
    targets = {t.chain.chain_id: t for t in fresh_targets(selected)}
    assert set(targets) == {CHAIN}
    assert isinstance(targets[CHAIN], ForkTarget) and targets[CHAIN].block == "latest"
    assert CHAIN in {t.chain.chain_id for t in fresh_targets(fork)}
    env = tmp_path / "provider.env"
    env.write_text("ALCHEMY_API_KEY=abcdef0123456789\n")
    require_providers(selected[0], env)
    require_providers(case_for("backtest/pinned/aave_v4_weth_supply/economic"), env)
    require_providers(case_for(f"fork/pinned/{SECOND_PUBLIC}/safe_roles/configured"), env)
    require_providers(next(c for c in CASES if c.lane == "faults"), env)
    env.write_text("UNRELATED=1\n")
    with pytest.raises(ValueError, match="ALCHEMY_API_KEY"):
        require_providers(selected[0], env)
    with pytest.raises(ValueError, match="env file is missing"):
        require_providers(selected[0], None)


def test_provider_fault_lane_runs_required_recovery_suites():
    faults = next(case for case in CASES if case.id == "fork/faults")
    proofs = "tests/strategy_fork/test_across_bridge.py"
    assert faults.nodes == (
        "tests/fork",
        "tests/strategy_fork/test_composed_outages.py",
        "tests/strategy_fork/test_multichain_outages.py",
        "tests/strategy_fork/test_production_rehearsals.py",
        f"{proofs}::test_an_unfilled_transfer_is_refunded_only_after_the_destination_proves_it",
        f"{proofs}::test_a_coordinator_killed_after_the_deposit_sends_no_second_one",
        f"{proofs}::test_the_source_bridges_while_the_destination_is_unavailable",
    )


def test_converted_fork_inputs_keep_the_author_file_and_add_an_operator(tmp_path):
    case = case_for("fork/fresh/aave_v4_weth_supply/safe_roles/configured")
    assert converted(case)
    path = resolved_author(case, tmp_path, ORIGINS)
    assert path.read_bytes() != (ROOT / "strategies/aave_v4_weth_supply/config.yaml").read_bytes()
    inputs = read_inputs(path).inputs
    source = read_inputs(ROOT / "strategies/aave_v4_weth_supply/config.yaml").inputs
    assert inputs == source
    operator = read_operator_config(tmp_path / "operator.yaml")
    assert operator.fork.anchors[0].height == ORIGINS[CHAIN]["number"]


def test_converted_backtest_settings_validate_against_the_public_readers(tmp_path):
    case = case_for("backtest/native/aave_v4_weth_supply/economic")
    resolved_author(case, tmp_path, {})
    settings = resolved_history(case, tmp_path, 2000000000)
    document = read_backtest_inputs(settings)
    assert document.inputs.mode == "economic"
    document.require_accounts(read_inputs(tmp_path / "config.yaml").inputs)
    assert set(document.inputs.inventory.funds) <= {"lending_0", "lending_1"}


def test_every_reference_uses_public_author_and_operator_inputs(tmp_path):
    fork = case_for(f"fork/fresh/{SECOND_PUBLIC}/safe_roles/configured")
    assert converted(fork)
    path = resolved_author(fork, tmp_path, ORIGINS)
    assert read_inputs(path).inputs
    assert (tmp_path / "operator.yaml").is_file()
    native = case_for(f"backtest/native/{SECOND_PUBLIC}/economic")
    directory = tmp_path / "native"
    directory.mkdir()
    resolved_author(native, directory, ORIGINS)
    assert resolved_history(native, directory, 2000000000).is_file()


def test_harness_run_case_sends_a_converted_reference_to_the_supervised_driver(
    tmp_path, monkeypatch
):
    from tests.strategy_fork import supervised
    from tests.strategy_fork.harness import Case, run_case

    calls = []
    monkeypatch.setattr(supervised, "run_supervised_case", lambda *args: calls.append(args))
    scenarios = ROOT / "tests/strategy_fork/scenarios"
    for name in ("aave_v4_weth_supply", SECOND_PUBLIC):
        source = Case(
            name,
            ROOT / "strategies" / name / "strategy.py",
            ROOT / "strategies" / name / "config.yaml",
            scenarios / name / "assertions.py",
        )
        run_case(source, None, {}, tmp_path, tmp_path / "artifacts", "configured")
    assert len(calls) == 2
    assert [call[1] for call in calls] == [
        ROOT / "examples/operators/aave_v4_weth_supply.fork.yaml",
        ROOT / f"examples/operators/{SECOND_PUBLIC}.fork.yaml",
    ]
    assert all(call[2:] == (None, tmp_path, tmp_path / "artifacts", "configured") for call in calls)
