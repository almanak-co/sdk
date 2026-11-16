"""Final hooks use generated fixture identities, never origin read placeholders."""

import json
from types import SimpleNamespace

import pytest

from almanak import checks
from almanak.configuration.admission import PreparedDeployment
from almanak.configuration.anchors import AnchorSelection
from almanak.configuration.fork_preparation import prepare_fork
from almanak.configuration.prepared import PreparationProgress
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.aave_v4.fork import AaveFork
from almanak.connectors.aave_v4.models import LendingParameters
from almanak.connectors.support import RunMode
from almanak.gateway.evm import native_metadata
from almanak.gateway.fixture_preparation import compile_fixture
from almanak.gateway.operator_config import content_digest
from almanak.gateway.operator_execution import PayerCeiling
from almanak.gateway.operator_fixture import EoaFixtureFund
from almanak.gateway.policy_resolution import check_fork_policy
from almanak.gateway.production_config import ForkGasExecution, GasPolicy, execution_gas
from almanak.models import AssetAmount
from almanak.state import Journal
from almanak.strategy import StrategyDefinition, ValidationRequest
from tests import test_fixture_accounts
from tests.test_author_resolution import (
    REFERENCE,
    frozen_cadence_reference,
    frozen_fork,
    resolve,
)
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_fixture_accounts import anchors as anchors
from tests.test_fixture_accounts import context as context
from tests.test_fixture_accounts import fixture as fixture
from tests.test_fixture_accounts import operator as operator
from tests.test_fixture_accounts import original as original
from tests.test_fixture_accounts import run as provision
from tests.test_prepared_configuration import forbid_hooks
from tests.test_resolution_context import revalidate

fixture_case = test_fixture_accounts.case


@pytest.fixture(params=["safe_roles", "eoa"])
def case(request, setup, fixture_case, tmp_path):
    fixture = fixture_case.operator.fork
    if request.param == "eoa":
        funds = tuple(
            EoaFixtureFund(
                kind="eoa",
                account=f.account,
                native_balance=fixture.native_balance(f),
                assets=f.assets,
            )
            for f in fixture.funds
        )
        fixture = revalidate(fixture, funds=funds, operators=())
    fixture = revalidate(
        fixture,
        anchors=(AnchorSelection(chain=setup.block.chain.chain_id, height=setup.block.number),),
    )
    setup.operator = revalidate(setup.operator, modes=(RunMode.FORK,), fork=fixture)
    document = read_inputs(frozen_cadence_reference(tmp_path))
    progress = resolve(document, setup)
    fixture_case.operator, fixture_case.context = setup.operator, progress.context
    fixture_case.plan = compile_fixture(progress.context, setup.operator, progress.result.anchors)
    fixture_case.nodes.config = fixture_case.plan
    fixture_case.node.origin = setup.block
    accounts = provision(fixture_case)
    return SimpleNamespace(
        document=document,
        progress=progress,
        fixture=accounts,
        setup=setup,
        definition=StrategyDefinition.read(REFERENCE / "strategy.py"),
        path=setup.root / "hooks.json",
        kind=request.param,
        nodes=fixture_case.nodes,
        directory=fixture_case.directory,
    )


def run(case):
    return prepare_fork(
        case.path, case.document, case.definition, case.progress, case.fixture, case.setup.operator
    )


def test_actual_reference_hooks_see_one_final_generated_account_parameter_object(case, monkeypatch):
    seen, validations = [], []
    worker, capability = checks.strategy_worker, checks.strategy_capabilities

    def observed(request):
        seen.append(request.config)
        if isinstance(request, ValidationRequest):
            validations.append(request)
        return worker(request)

    def observed_capability(definition, parameters):
        seen.append(parameters)
        return capability(definition, parameters)

    monkeypatch.setattr(checks, "strategy_worker", observed)
    monkeypatch.setattr(checks, "strategy_capabilities", observed_capability)
    record = run(case)
    prepared = record.prepared
    assert len(seen) == 3 and len(validations) == 1
    assert all(p is prepared.parameters for p in seen)
    assert prepared.parameters.accounts == case.fixture.accounts
    assert prepared.parameters.accounts != case.progress.result.binding.accounts
    expected = AaveFork.prepare_chains(
        frozen_fork(),
        case.fixture.accounts.bindings,
        case.fixture.bindings,
    )
    # Fork preparation re-derives chains and accounts; the authored knobs pass through.
    expected_parameters = LendingParameters.model_validate(
        {
            **{name: getattr(expected, name) for name in LendingParameters.model_fields},
            "parameters": frozen_fork().parameters,
        }
    )
    assert prepared.parameters == expected_parameters
    assert prepared.fixture_digest == case.fixture.digest
    assert len(prepared.capabilities.requirements) == 6
    frozen = case.path.read_bytes(), case.path.stat().st_mtime_ns
    before = len(case.setup.rpc.calls)
    forbid_hooks(monkeypatch)
    assert run(case) == record
    assert (case.path.read_bytes(), case.path.stat().st_mtime_ns) == frozen
    assert len(case.setup.rpc.calls) == before
    with pytest.raises(ValueError, match="exact generated fixture accounts"):
        prepared.require(case.document, case.definition, case.progress)


def test_durable_deployment_preserves_fixture_and_frozen_hooks(case, monkeypatch):
    record = run(case)
    bundle = PreparedDeployment(resolution=case.progress, preparation=record, fixture=case.fixture)
    config = bundle.configuration()
    bundle.require_operator(case.setup.operator)
    record.prepared.require_configuration(config, case.definition)
    forbid_hooks(monkeypatch)
    monkeypatch.setattr(
        "almanak.runtime.strategy_capabilities", lambda *_: pytest.fail("repeated capabilities")
    )
    journal = Journal(case.setup.root / "state.sqlite")
    journal.create(config, case.definition, preparation=bundle)
    first = journal.load()
    assert journal.configuration_preparation() == bundle
    bundle.require_checkpoint(first)
    assert first.config.accounts == case.fixture.accounts
    assert first.capabilities == record.prepared.capabilities
    assert PreparedDeployment.model_validate_json(bundle.model_dump_json()) == bundle
    changed = revalidate(case.setup.operator, modes=(RunMode.FORK, RunMode.PRODUCTION))
    with pytest.raises(ValueError):
        bundle.require_operator(changed)


def test_provisional_hook_results_cannot_be_relabelled_as_final_fixture_authority(case):
    from almanak.configuration.prepared import PreparationProgress, build_prepared

    provisional = build_prepared(case.document, case.definition, case.progress)
    record = PreparationProgress(prepared=provisional, prepared_digest=provisional.digest)
    with pytest.raises(ValueError, match="generated account binding"):
        PreparedDeployment(resolution=case.progress, preparation=record, fixture=case.fixture)


def test_missing_or_changed_fixture_refuses_before_hooks(case, monkeypatch):
    forbid_hooks(monkeypatch)
    wrong = revalidate(case.progress.context, identity="c" * 32)
    progress = SimpleNamespace(context=wrong, result=case.progress.result)
    with pytest.raises(ValueError, match="resolution context"):
        prepare_fork(
            case.path, case.document, case.definition, progress, case.fixture, case.setup.operator
        )
    assert not case.path.exists()


def test_retained_hooks_cannot_change_their_fixture_evidence(case, monkeypatch):
    run(case)
    frozen = case.path.read_bytes()
    forbid_hooks(monkeypatch)
    case.fixture = revalidate(case.fixture, installation_digest="f" * 64)
    with pytest.raises(ValueError, match="exact generated fixture accounts"):
        run(case)
    assert case.path.read_bytes() == frozen


def fixture_gas(case):
    policies, ceilings = [], []
    for index, names in enumerate(sorted(case.setup.operator.fork.payer_groups(), key=sorted)):
        name = sorted(names)[0]
        chain = case.setup.operator.fork.chain(
            next(f for f in case.setup.operator.fork.funds if f.account == name)
        )
        asset, _ = native_metadata(chain)
        policy = GasPolicy(
            name=f"payer_{index}",
            accounts=tuple(sorted(names)),
            asset=asset,
            source="network_rpc_fee_history",
            reward_percentile=50,
            base_fee_multiplier_bps=20000,
            max_gas_price_wei=100,
            max_gas_limit=1000,
            max_transaction_fee=AssetAmount(asset=asset, amount=100_000),
            total_budget=AssetAmount(asset=asset, amount=1_000_000),
            exit_reserve=AssetAmount(asset=asset, amount=100_000),
            gas_limit_buffer_bps=2500,
        )
        policies.append(policy)
        ceilings.append(
            PayerCeiling(
                accounts=policy.accounts,
                max_gas_price_wei=policy.max_gas_price_wei,
                max_gas_limit=policy.max_gas_limit,
                max_transaction_fee=policy.max_transaction_fee,
                total_budget=policy.total_budget,
                allow_replacement=False,
            )
        )
    return tuple(policies), tuple(ceilings)


def test_generated_fixture_payers_bind_exact_fork_policy_without_production(case):
    policies, ceilings = fixture_gas(case)
    operator = revalidate(
        case.setup.operator, fork=revalidate(case.setup.operator.fork, gas=ceilings)
    )
    context = revalidate(case.progress.context, operator_digest=operator.digest)
    compatibility = check_fork_policy(context, operator, case.fixture.accounts, policies)
    assert compatibility is not None
    assert compatibility.accounts == case.fixture.accounts
    assert compatibility.execution == ForkGasExecution(gas=policies)

    record = run(case)
    bundle = PreparedDeployment(resolution=case.progress, preparation=record, fixture=case.fixture)
    base = bundle.configuration()
    config = type(base).model_validate(
        {
            **base.model_dump(),
            "gas": policies,
            "fork_gas": compatibility.execution,
        }
    )
    assert config.production is None
    assert execution_gas(config) == compatibility.execution

    parameters = type(record.prepared.parameters).model_validate(
        {**record.prepared.parameters.model_dump(), "gas": policies}
    )
    raw = json.loads(record.prepared.model_dump_json())
    raw.update(
        context_digest=context.digest,
        parameters=json.loads(parameters.model_dump_json()),
        parameters_digest=content_digest(parameters),
    )
    prepared = type(record.prepared).model_validate_json(json.dumps(raw))
    retained = PreparationProgress(
        prepared=prepared, prepared_digest=prepared.digest, policy=compatibility
    )
    assert PreparationProgress.model_validate_json(retained.model_dump_json()) == retained
