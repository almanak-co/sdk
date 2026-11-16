"""The fork adapter assembles the legacy fork configuration from resolved public inputs."""

import gzip
import json
from types import SimpleNamespace

import pytest
import yaml

from almanak.chains import CheckpointReference
from almanak.configuration.anchors import AnchorSelection
from almanak.configuration.bootstrap import ResolutionProgress
from almanak.configuration.resolution import ResolutionAnchors
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.connectors.curve.resolution import resolve_market as resolve_curve_market
from almanak.connectors.support import RunMode
from almanak.gateway.bootstrap import MarketFacts, ReadBinding, ResolutionResult
from almanak.gateway.fork_config import ForkSafeFund
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.fork.support import start_gateway
from tests.strategy_fork import adapter
from tests.strategy_fork.adapter import fork_config_for, resolve_reference
from tests.strategy_fork.harness import Case, case_fork_config
from tests.test_author_resolution import ROOT, frozen_cadence_reference, frozen_fork, resolve
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_operator_fixture import fixture as fixture
from tests.test_operator_fixture import original as original
from tests.test_resolution_context import revalidate


def test_public_inputs_and_one_resolution_reproduce_the_frozen_fork_positions(
    setup, fixture, tmp_path
):
    anchors = (AnchorSelection(chain=setup.block.chain.chain_id, height=setup.block.number),)
    setup.operator = revalidate(
        setup.operator, modes=(RunMode.FORK,), fork=revalidate(fixture, anchors=anchors)
    )
    operator_path = tmp_path / "operator.yaml"
    operator_path.write_text(yaml.safe_dump(setup.operator.model_dump(mode="json")))
    reference = frozen_cadence_reference(tmp_path)
    progress = resolve(read_inputs(reference), setup)
    calls = len(setup.rpc.calls)
    config = fork_config_for(reference, operator_path, progress)
    assert len(setup.rpc.calls) == calls
    frozen = frozen_fork()
    assert type(config) is ForkConfig
    assert (config.allocations, config.debts) == (frozen.allocations, frozen.debts)
    assert config.decision_trigger == frozen.decision_trigger
    assert config.asset_metadata == frozen.asset_metadata
    assert [t.chain.chain_id for t in config.forks] == [setup.block.chain.chain_id]
    assert config.forks[0].block.number == setup.block.number
    assert {f.account for f in config.funds} == {a.account for a in frozen.allocations}
    assert all(isinstance(f, ForkSafeFund) for f in config.funds)
    assert {o.name for o in config.operators} == {f.operator for f in frozen.funds}
    assert config.production is None
    other = tmp_path / "other.yaml"
    changed = revalidate(setup.operator, modes=(RunMode.FORK, RunMode.BACKTEST))
    other.write_text(yaml.safe_dump(changed.model_dump(mode="json")))
    with pytest.raises(ValueError, match="differs from the selected operator"):
        fork_config_for(reference, other, progress)


CURVE = ROOT / "strategies/curve_ethereum_stable_lp"
CURVE_OPERATOR = ROOT / "examples/operators/curve_ethereum_stable_lp.fork.yaml"
CURVE_READS = ROOT / "tests/fixtures/curve-ethereum-resolution-reads.json.gz"


def recorded_curve_resolution():
    """A retained Curve resolution replayed from recorded reads, with no gateway process."""
    raw = json.loads(gzip.decompress(CURVE_READS.read_bytes()))
    block = Block.model_validate_json(json.dumps(raw["block"]))
    rows = tuple(ResolutionRead.model_validate_json(json.dumps(row)) for row in raw["evidence"])
    document = read_inputs(CURVE / "config.yaml")
    operator = read_operator_config(CURVE_OPERATOR)
    context = document.inputs.context(operator, RunMode.FORK, operator.fork.anchors)
    binding = ReadBinding(
        context_digest=context.digest,
        operator_digest=context.operator_digest,
        mode=RunMode.FORK,
        accounts=context.match_operator(operator, RunMode.FORK),
    )
    checkpoint = CheckpointReference.model_validate_json(
        json.dumps(
            {
                "chain": block.chain.model_dump(mode="json"),
                "reference": block.hash,
                "timestamp": block.timestamp,
                "height": block.number,
            }
        )
    )
    reader = RetainedResolutionReader(block.chain, rows)
    try:
        markets = tuple(
            MarketFacts(
                name=market.name,
                resolved=resolve_curve_market(reader, market.selection, block),
                evidence=rows,
            )
            for market in context.markets
        )
    finally:
        reader.__exit__()
    result = ResolutionResult(
        binding=binding,
        anchors=ResolutionAnchors(context_digest=context.digest, checkpoints=(checkpoint,)),
        markets=markets,
    )
    return ResolutionProgress(
        context=context,
        binding=binding,
        checkpoints=(checkpoint,),
        result=result,
        result_digest=result.digest,
    )


def test_public_curve_inputs_and_one_resolution_reproduce_the_frozen_fork_positions():
    from almanak.connectors.curve.fork import ForkConfig as CurveForkConfig
    from scripts.legacy_configuration.author import read_author
    from tests.legacy_references import harness_directory

    config = fork_config_for(CURVE / "config.yaml", CURVE_OPERATOR, recorded_curve_resolution())
    frozen = read_author(harness_directory(CURVE.name) / "config.yaml").fork_config()
    assert type(config) is CurveForkConfig
    assert config.allocations == frozen.allocations
    # The frozen harness decides every 5 seconds; the reference at the live 60.
    assert config.decision_trigger == frozen.decision_trigger.model_copy(update={"seconds": 60})
    assert config.asset_metadata == frozen.asset_metadata
    assert [t.chain.chain_id for t in config.forks] == [t.chain.chain_id for t in frozen.forks]
    assert [t.block.number for t in config.forks] == [t.block.number for t in frozen.forks]
    assert {f.account for f in config.funds} == {f.account for f in frozen.funds}
    assert all(isinstance(f, ForkSafeFund) for f in config.funds)
    assert {o.name for o in config.operators} == {o.name for o in frozen.operators}


def test_legacy_fork_adapter_refuses_to_discard_authored_gas(monkeypatch):
    monkeypatch.setattr(
        adapter,
        "construct_parameters",
        lambda *_: SimpleNamespace(gas=(object(),)),
    )
    with pytest.raises(ValueError, match="require admitted fork preparation"):
        fork_config_for(CURVE / "config.yaml", CURVE_OPERATOR, recorded_curve_resolution())


def test_fixture_plan_checks_authored_gas_before_compiling(monkeypatch):
    name = "uniswap_v4_avalanche_trading"
    config = ROOT / "strategies" / name / "config.yaml"
    operator = ROOT / "examples" / "operators" / f"{name}.fork.yaml"
    checked = []
    monkeypatch.setattr(
        adapter, "require_fork_policy", lambda document, bound: checked.append((document, bound))
    )
    monkeypatch.setattr(adapter, "compile_fixture", lambda *args: "plan")
    progress = SimpleNamespace(result=SimpleNamespace(anchors=object()), context=object())
    assert adapter.fixture_plan_for(config, operator, progress) == "plan"
    assert checked and checked[0][0].inputs.strategy.gas
    assert checked[0][1].fork.gas


def test_test_gateway_uses_retained_preparation_for_admitted_fork_gas(tmp_path):
    directory = tmp_path / "deployment"
    directory.mkdir()
    (directory / "prepared-deployment.json").write_text("{}")
    launched = []
    process = SimpleNamespace()

    def launch(arguments, **options):
        launched.append((arguments, options))
        return process

    fork = SimpleNamespace(rpc=SimpleNamespace(endpoint="http://127.0.0.1:1"))
    _, runtime = start_gateway(fork, directory, launch, lease_seconds=30)
    command = launched[0][0]
    assert "--gateway-config" in command and "--preparation-file" in command
    assert "--config" not in command
    assert "--config" in runtime


def test_reference_resolution_continues_a_retained_partial_progress(tmp_path, monkeypatch):
    _, _, context = adapter.fork_context(CURVE / "config.yaml", CURVE_OPERATOR)
    partial = ResolutionProgress(context=context)
    directory = tmp_path / "resolution"
    directory.mkdir()
    path = directory / "resolution.json"
    path.write_text(partial.model_dump_json())
    calls = []
    continued = []

    class Gateway:
        def __init__(self, *args):
            calls.append(args)

        def __enter__(self):
            return object()

        def __exit__(self, *args):
            return None

    def finish(selected_path, context, client):
        assert selected_path == path
        assert context == partial.context
        assert client is not None
        continued.append(True)

    monkeypatch.setattr(adapter, "ReadGateway", Gateway)
    monkeypatch.setattr(adapter, "resolve_configuration", finish)
    resumed = resolve_reference(CURVE / "config.yaml", CURVE_OPERATOR, tmp_path / ".env", directory)
    assert calls
    assert continued == [True]
    assert resumed == partial


def test_converted_case_uses_public_resolution_adapter(tmp_path, monkeypatch):
    case = Case(
        CURVE.name,
        CURVE / "strategy.py",
        CURVE / "config.yaml",
        ROOT / "tests/strategy_fork/scenarios" / CURVE.name / "assertions.py",
    )
    progress = recorded_curve_resolution()
    calls = []

    def resolve(config, operator, env, directory):
        calls.append((config, operator, env, directory))
        return progress

    expected = object()
    monkeypatch.setattr(adapter, "resolve_reference", resolve)
    monkeypatch.setattr(adapter, "fork_config_for", lambda *args: expected)
    env = tmp_path / ".env"
    directory = tmp_path / "resolution"
    assert case_fork_config(case, env, directory) is expected
    assert calls == [
        (
            case.config,
            ROOT / "examples/operators/curve_ethereum_stable_lp.fork.yaml",
            env,
            directory,
        )
    ]
