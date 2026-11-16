"""`strategy.parameters`: author knobs checked by shape and read by the strategy as given."""

import importlib
import json
from argparse import Namespace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from almanak import testflow
from almanak.backtest.economic_model import parameters as economic_parameters
from almanak.backtest.economic_state import (
    EconomicEvent,
    EconomicEventInput,
    EconomicParameters,
)
from almanak.checks import author_inputs
from almanak.cli_arguments import build_parser
from almanak.configuration.parameter_codec import hook_parameters
from almanak.configuration.parameters import construct_parameters
from almanak.configuration.preparation import check_preparation
from almanak.configuration.strategy_parameters import (
    StrategyParameters,
    Written,
    like,
    overrides,
)
from almanak.configuration.unresolved import read_frozen_inputs, read_inputs
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.connectors.aave_v4.models import DecisionInput, OutcomeInput
from almanak.effects import Effects
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.provisioning import production_configuration
from almanak.runtime import strategy_decision, strategy_worker
from almanak.strategy import EventRequest, OutcomeRequest, StrategyDefinition
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.triggers import IntervalSelection
from tests.test_author_resolution import REFERENCE, parsed, resolve
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import raw as raw
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_dx_schemas import validator
from tests.test_testflow_lifecycle import Process, workspace

# The reference's own knobs, which its strategy validates, plus one of each kind.
KNOBS = {
    "borrow_quantity": "5",
    "unwind_margin": "0.25",
    "loops": 2,
    "ltv": 0.75,
    "label": "wide",
    "dry_run": False,
    "steps": [1, "0.5", None],
}
SEEN = {**KNOBS, "steps": (1, "0.5", None)}
# Every hook asserts it sees the knobs, read-only, then defers to the reference. The
# reference's typed model is hidden from the SDK, so the hooks see the plain mapping, and
# the reference's own hooks get its knobs typed as they expect.
OBSERVING = f"""
_validate_config, _capabilities, _decide, _on_outcome = (
    validate_config, capabilities, decide, on_outcome
)
_REFERENCE_KNOBS = [StrategyParams]
del StrategyParams


def typed(config):
    values = {{key: config.parameters[key] for key in ("borrow_quantity", "unwind_margin")}}
    knobs = _REFERENCE_KNOBS[0].model_validate(values, strict=False)
    return config.model_copy(update={{"parameters": knobs}})


def typed_context(context):
    return context.model_copy(update={{"config": typed(context.config)}})


def seen(config):
    assert dict(config.parameters) == {SEEN!r}, config.parameters
    try:
        config.parameters["loops"] = 0
    except TypeError:
        return
    raise AssertionError("parameters are writable")


def validate_config(config):
    seen(config)
    _validate_config(typed(config))


def capabilities(config):
    seen(config)
    return _capabilities(typed(config))


def decide(context):
    seen(context.config)
    return _decide(typed_context(context))


def on_outcome(context):
    seen(context.config)
    return _on_outcome(typed_context(context))
"""


def observing():
    reference = StrategyDefinition.read(REFERENCE / "strategy.py")
    return StrategyDefinition(filename="strategy.py", source=reference.source + OBSERVING)


def with_knobs(raw, knobs=KNOBS):
    raw["strategy"]["parameters"] = knobs
    return raw


def test_parameters_are_read_only_and_lists_arrive_as_tuples(raw):
    parameters = parsed(with_knobs(raw)).inputs.strategy.parameters
    assert isinstance(parameters, StrategyParameters)
    assert dict(parameters) == SEEN
    assert isinstance(parameters["ltv"], float) and parameters["steps"][1] == "0.5"
    with pytest.raises(TypeError):
        parameters["loops"] = 3  # type: ignore[index]


@pytest.mark.parametrize(
    ("knobs", "message"),
    [
        ({"Loops": 1}, "should match pattern"),
        ({"1loops": 1}, "should match pattern"),
        ({"max-loops": 1}, "should match pattern"),
        ({"loops": [[1]]}, "flat list of scalars"),
        ({"loops": {"count": 1}}, "flat list of scalars"),
        ([1, 2], "Input should be an object"),
    ],
)
def test_check_refuses_malformed_parameters_at_their_source(raw, knobs, message):
    with pytest.raises(ConfigurationError, match=message) as error:
        parsed(with_knobs(raw, knobs))
    assert error.value.field.startswith("strategy.parameters")
    assert error.value.line is not None


def test_editor_schema_matches_the_shape_rule(raw):
    assert not list(validator("strategy.schema.json").iter_errors(with_knobs(raw)))
    for knobs in ({"Loops": 1}, {"loops": [[1]]}, {"loops": {"a": 1}}):
        assert list(validator("strategy.schema.json").iter_errors(with_knobs(raw, knobs)))


def test_absent_or_empty_parameters_keep_every_digest(raw, setup):
    raw["strategy"].pop("parameters", None)
    absent = parsed(raw)
    empty = parsed(with_knobs(json.loads(json.dumps(raw)), {}))
    knobs = parsed(with_knobs(json.loads(json.dumps(raw))))
    assert "parameters" not in absent.inputs.model_dump(mode="json")["strategy"]
    assert empty.inputs.digest == absent.inputs.digest != knobs.inputs.digest
    progress = resolve(absent, setup)
    resolved = construct_parameters(absent, progress)
    assert "parameters" not in resolved.model_dump(mode="json")
    assert resolved == construct_parameters(empty, progress)


def test_resolution_carries_the_knobs_to_validate_config_and_capabilities(raw, setup):
    document = parsed(with_knobs(raw))
    progress = resolve(document, setup)
    resolved = construct_parameters(document, progress)
    assert dict(resolved.parameters) == SEEN
    assert resolved.model_dump(mode="json")["parameters"] == KNOBS
    report = check_preparation(document, observing(), progress)
    states = {s["stage"]: s["status"] for s in report["stages"]}
    assert states["strategy"] == states["capabilities"] == "passed"


def test_decide_and_on_outcome_read_the_knobs_through_the_worker(recorded):
    config = recorded.config.model_copy(update={"parameters": StrategyParameters(SEEN)})
    context = DecisionInput(
        config=config,
        memory=Memory(),
        owned=recorded.owned,
        observations=recorded.baseline,
        trigger=recorded.outcomes[0].decision.trigger,
    )
    allocation = config.allocations[0]
    decision = strategy_decision(observing(), context)
    assert decision.intent == config.supply(allocation.name, allocation.capital)
    outcome = OutcomeInput(
        config=config, memory=recorded.memory, result=recorded.outcomes[0].strategy_result()
    )
    strategy_worker(OutcomeRequest(strategy=observing(), context=outcome))


def test_backtest_parameters_keep_the_knobs_for_on_event(recorded):
    config = recorded.config.model_copy(update={"parameters": StrategyParameters(SEEN)})
    author = hook_parameters(config)
    interval = IntervalSelection(configured=author.decision_trigger)
    config = economic_parameters(None, author, interval)
    assert isinstance(config, EconomicParameters) and dict(config.parameters) == SEEN
    source = (
        f"def on_event(context):\n"
        f"    assert dict(context.config.parameters) == {SEEN!r}\n"
        f"    return context.memory\n"
    )
    request = EventRequest(
        strategy=StrategyDefinition(filename="strategy.py", source=source),
        context=EconomicEventInput(
            config=config,
            memory=Memory(),
            event=EconomicEvent(
                effective_at=datetime(2026, 4, 1, tzinfo=UTC),
                liquidations=(),
                deficits=(),
                simulation=SimulationEvidence(execution_id="event:0", model="fixture", inputs=()),
            ),
            effects=Effects(),
        ),
    )
    assert Memory.model_validate_json(strategy_worker(request)) == Memory()


@pytest.mark.parametrize(
    ("pair", "value"),
    [
        ("loops=1", 1),
        ("ltv=0.5", 0.5),
        ('ltv="0.5"', "0.5"),
        ("dry_run=true", True),
        ("label=a=b", "a=b"),
        ("label=", None),
    ],
)
def test_param_values_are_yaml_scalars_split_at_the_first_equals(pair, value):
    key = pair.split("=", 1)[0]
    assert overrides([pair]) == {key: value}
    assert type(overrides([pair])[key]) is (Written if type(value) is float else type(value))


@pytest.mark.parametrize("pair", ["loops", "=1", "steps=[1, 2]", "a=b: c", "loops=1"])
def test_malformed_param_is_refused(pair):
    pairs = [pair, pair] if pair == "loops=1" else [pair]
    with pytest.raises(ConfigurationError) as error:
        overrides(pairs)
    assert error.value.code == "invalid_parameter_override"


def source_workspace(tmp_path, raw):
    directory = tmp_path / "source"
    directory.mkdir()
    (directory / "config.yaml").write_text(yaml.safe_dump(with_knobs(raw), sort_keys=False))
    # A strategy without a typed model: overrides take the configured types.
    (directory / "strategy.py").write_text("def decide(context):\n    return None\n")
    return directory


def test_override_replaces_only_the_workspace_copy(raw, tmp_path):
    directory = source_workspace(tmp_path, raw)
    before = (directory / "config.yaml").read_bytes()
    document, _ = author_inputs(
        Namespace(directory=directory), overrides=overrides(["loops=1", "ltv=0.25"])
    )
    assert dict(document.inputs.strategy.parameters) == {**SEEN, "loops": 1, "ltv": 0.25}
    assert (directory / "config.yaml").read_bytes() == before
    copy = tmp_path / "author-inputs.json"
    copy.write_text(json.dumps(document.inputs.model_dump(mode="json")))
    assert read_inputs(copy).inputs == document.inputs


@pytest.mark.parametrize(
    ("pair", "key", "value"),
    [
        ('loops="1"', "loops", 1),
        ("loops=1.0", "loops", 1),
        ('ltv="0.25"', "ltv", 0.25),
        ("ltv=1", "ltv", 1.0),
        ("label=3", "label", "3"),
        ("label=0.5", "label", "0.5"),
        ('dry_run="true"', "dry_run", True),
        ("loops=12345678901234567891", "loops", 12345678901234567891),
        ('loops="12345678901234567891"', "loops", 12345678901234567891),
        ("loops=9007199254740993.0", "loops", 9007199254740993),
        ("label=1.10", "label", "1.10"),
        ("borrow_quantity=1.123456789012345678", "borrow_quantity", "1.123456789012345678"),
        ("ltv=0.00001", "ltv", 0.00001),
        ('ltv="0.00001"', "ltv", 0.00001),
        ("ltv=1e-5", "ltv", 0.00001),
        ("ltv=10000000000000000.0", "ltv", 1e16),
        ('ltv="1e16"', "ltv", 1e16),
    ],
)
def test_override_takes_the_configured_type(raw, tmp_path, pair, key, value):
    directory = source_workspace(tmp_path, raw)
    document, _ = author_inputs(Namespace(directory=directory), overrides=overrides([pair]))
    seen = document.inputs.strategy.parameters[key]
    assert seen == value and type(seen) is type(value)
    # The workspace freezes the inputs as JSON, where 1e-05 has no decimal point.
    copy = tmp_path / "author-inputs.json"
    copy.write_text(json.dumps(document.inputs.model_dump(mode="json")))
    assert read_frozen_inputs(copy).inputs == document.inputs


@pytest.mark.parametrize(
    "pair",
    [
        'loops="one"',
        "loops=1.5",
        "dry_run=1",
        "ltv=yes",
        'ltv="inf"',
        'ltv="nan"',
        "loops=1_000",
        "loops=1e3",
        'loops="1e3"',
        "loops=1.0e3",
        "ltv=1_0.5",
        'ltv="1_0.5"',
    ],
)
def test_override_of_another_type_is_refused(raw, tmp_path, pair):
    directory = source_workspace(tmp_path, raw)
    with pytest.raises(ConfigurationError, match="does not match the configured") as refused:
        author_inputs(Namespace(directory=directory), overrides=overrides([pair]))
    assert refused.value.code == "invalid_parameter_override"


@pytest.mark.parametrize("pair", ["loops=", "loops=null", "label=~", "steps=null"])
def test_a_null_override_of_a_set_parameter_is_refused(raw, tmp_path, pair):
    directory = source_workspace(tmp_path, raw)
    key = pair.partition("=")[0]
    with pytest.raises(ConfigurationError, match=f"{key} is null") as refused:
        author_inputs(Namespace(directory=directory), overrides=overrides([pair]))
    assert (refused.value.code, refused.value.field) == (
        "invalid_parameter_override",
        f"strategy.parameters.{key}",
    )
    assert like(None, None, "unset") is None


def test_a_frozen_exponent_float_reads_back_as_a_float(raw):
    knobs = {**KNOBS, "ltv": 1.0e-5, "steps": [1e16]}
    assert dict(parsed(with_knobs(raw, knobs)).inputs.strategy.parameters)["ltv"] == 1e-05


def test_a_list_of_one_type_takes_that_type_element_wise():
    assert like((1, 2), ("3", 4.0), "steps") == (3, 4)
    assert like((0.5, None), (1, "0.25"), "fractions") == (1.0, 0.25)
    assert like((1, None), (None, "2"), "steps") == (None, 2)
    assert like((1, "a"), ("3", 4.0), "mixed") == ("3", 4.0)
    with pytest.raises(ConfigurationError, match="does not match the configured"):
        like((1, 2), ("x",), "steps")
    with pytest.raises(ConfigurationError, match="does not match the configured"):
        like(0.5, float("inf"), "ltv")
    with pytest.raises(ConfigurationError, match="does not match the configured"):
        like(0.5, 10**400, "ltv")


def test_a_scalar_for_a_list_parameter_is_refused():
    with pytest.raises(ConfigurationError, match="steps=3 is not a list") as refused:
        like((1, 2), 3, "steps")
    assert refused.value.field == "strategy.parameters.steps"
    assert refused.value.code == "invalid_parameter_override"
    with pytest.raises(ConfigurationError, match="is not a list"):
        like((1, "a"), "x", "mixed")


@pytest.mark.parametrize(
    ("text", "value"),
    [
        ("9007199254740993.0", 9007199254740993),
        ("2.0", 2),
        ("7" * 400, int("7" * 400)),
    ],
)
def test_an_integer_override_is_read_exactly(text, value):
    assert like(1, text, "loops") == value


@pytest.mark.parametrize(
    "text", ["1.0000000000000001", "1e999999999", "0x10", "Infinity", "1e3", "1_000", "1E3"]
)
def test_an_integer_override_that_is_not_exactly_one_is_refused(text):
    with pytest.raises(ConfigurationError, match="does not match the configured int"):
        like(1, text, "loops")


def test_unknown_param_is_refused_as_the_admission_with_no_workspace(raw, tmp_path):
    directory = source_workspace(tmp_path, raw)
    before = (directory / "config.yaml").read_bytes()
    args = Namespace(
        directory=directory,
        config=None,
        output=tmp_path / "run",
        param=["loop=1"],
        production_parameters=False,
    )
    verdict = testflow.empty_verdict()
    assert testflow.start(args, args.output, verdict) is None
    refusal = verdict["admission"]["refusal"]
    assert refusal["error"] == "invalid_config"
    assert refusal["field"] == "strategy.parameters.loop"
    assert "Configured parameters: borrow_quantity, unwind_margin, loops, ltv" in refusal["message"]
    assert verdict["parameter_overrides"] == {}
    assert not args.output.exists()
    assert (directory / "config.yaml").read_bytes() == before
    with pytest.raises(ConfigurationError, match="not in strategy.parameters"):
        author_inputs(Namespace(directory=directory), overrides={"loop": 1})


def test_verdict_records_the_applied_overrides(tmp_path, monkeypatch):
    received = {}

    def prepare(args, *, test_clock, overrides, fields):
        received.update(overrides)
        root = workspace(tmp_path)
        (root / "private").mkdir()
        inputs = {"strategy": {"parameters": overrides}}
        (root / "private/author-inputs.json").write_text(json.dumps(inputs))
        return root

    monkeypatch.setattr("almanak.workspace.prepare", prepare)
    monkeypatch.setattr("almanak.workspace.launch", lambda root: Process())
    args = Namespace(
        output=tmp_path / "run", param=["loops=1", 'label="x"'], production_parameters=False
    )
    verdict = testflow.empty_verdict()
    assert isinstance(testflow.start(args, args.output, verdict), Process)
    assert received == verdict["parameter_overrides"] == {"loops": 1, "label": "x"}
    assert testflow.empty_verdict()["parameter_overrides"] == {}


def test_verdict_echoes_the_coerced_value_the_workspace_froze(raw, tmp_path, monkeypatch):
    directory = source_workspace(tmp_path, raw)
    verdict, _ = started(tmp_path, monkeypatch, directory, param=['loops="1"', "ltv=0.3"])
    # The tool layer's "1" reaches the strategy as the declared int, and the verdict says so.
    assert verdict["parameter_overrides"] == {"loops": 1, "ltv": 0.3}
    assert type(verdict["parameter_overrides"]["loops"]) is int


def test_test_run_parses_the_parameter_options():
    required = ["test", "run", "--gateway-config", "g.yaml", "--env-file", ".env", "--output", "o"]
    args = build_parser().parse_args(
        [*required, "--param", "loops=1", "--param", "label=x", "--production-parameters"]
    )
    assert (args.param, args.production_parameters) == (["loops=1", "label=x"], True)
    args = build_parser().parse_args(required)
    assert (args.param, args.production_parameters) == (None, False)


@pytest.mark.parametrize("parameters", [{"borrow_quantity": "5", "loops": "1"}, {}])
def test_a_fork_fixture_deployment_keeps_the_parameters(tmp_path, parameters):
    fixture = Path(__file__).parent / "fixtures/aave-v4-interval-config.json"
    config = ForkConfig.model_validate_json(fixture.read_text()).model_copy(
        update={"parameters": StrategyParameters(parameters)}
    )
    node = object.__new__(Fork)
    node.origin = config.origin
    node.metadata = {"instanceId": "0x" + "12" * 32}
    node.rpc = SimpleNamespace(endpoint="http://127.0.0.1:1234")
    node.funded_signer = lambda *args, **kwargs: SimpleNamespace(
        address="0x" + "34" * 20, key=bytes.fromhex("56" * 32)
    )
    deployment = node.prepare(config, tmp_path / "deployment")
    assert dict(deployment.parameters) == parameters
    written = json.loads((tmp_path / "deployment/deployment.json").read_text())
    assert written.get("parameters", {}) == parameters


FORK_SITES = [
    ("aave_v3", "AaveFork", "Config", "prepare_chains"),
    ("aave_v4", "AaveFork", "LendingConfig", "prepare_chains"),
    ("compound_v3", "CompoundFork", "Config", "prepare_chains"),
    ("composed", "ComposedFork", "Config", "prepare_chains"),
    ("gmx_v2", "Fork", "Config", "prepare_chains"),
    ("morpho_blue", "MorphoFork", "Config", "prepare_chains"),
    ("sparklend", "SparkFork", "Config", "prepare_chains"),
    ("uniswap_v3", "UniswapFork", "config_type", "prepare_chains"),
    ("uniswap_v4", "UniswapFork", "Config", "prepare_chains"),
    ("uniswap_v4", "UniswapFork", "Config", "prepare"),
]


@pytest.mark.parametrize(("connector", "name", "config", "method"), FORK_SITES)
def test_every_fork_deployment_keeps_the_parameters(monkeypatch, connector, name, config, method):
    module = importlib.import_module(f"almanak.connectors.{connector}.fork")
    fork = getattr(module, name)
    # The shared dex fork builds its connector's config through the class's config_type.
    monkeypatch.setattr(fork if config == "config_type" else module, config, dict)
    parameters = StrategyParameters({"loops": 1})
    author = SimpleNamespace(
        decision_trigger=None,
        asset_metadata={},
        allocations=(),
        debts=(),
        inventory=(),
        parameters=parameters,
    )
    arguments = (None, None, None) if method == "prepare" else ((),)
    assert getattr(fork, method)(author, (), *arguments)["parameters"] is parameters


def test_a_production_deployment_keeps_the_parameters(monkeypatch):
    codec = SimpleNamespace(config=dict)
    monkeypatch.setattr(
        "almanak.gateway.provisioning.connector_for", lambda author: SimpleNamespace(codec=codec)
    )
    parameters = StrategyParameters({"loops": 1})
    author = SimpleNamespace(
        production=object(),
        allocations=(),
        decision_trigger=None,
        asset_metadata={},
        parameters=parameters,
    )
    assert production_configuration(author, ())["parameters"] is parameters


def with_tests(raw, tests):
    raw = with_knobs(raw)
    raw["strategy"]["test_parameters"] = tests
    return raw


def test_test_parameters_take_the_declared_type_and_leave_the_inputs(raw, setup):
    tests = {"loops": "1", "ltv": 1, "label": 3, "dry_run": "true"}
    document = parsed(with_tests(raw, tests))
    assert document.test_parameters == {"loops": 1, "ltv": 1.0, "label": "3", "dry_run": True}
    assert type(document.test_parameters["loops"]) is int
    # The strategy, backtests and digests see only strategy.parameters.
    assert dict(document.inputs.strategy.parameters) == SEEN
    assert "test_parameters" not in json.dumps(document.inputs.model_dump(mode="json"))
    plain = parsed(with_knobs(json.loads(json.dumps(raw))))
    assert document.inputs.digest == plain.inputs.digest
    progress = resolve(document, setup)
    assert construct_parameters(document, progress) == construct_parameters(plain, progress)


@pytest.mark.parametrize(
    ("tests", "field", "message"),
    [
        ({"loop": 1}, "strategy.test_parameters.loop", "not in strategy.parameters"),
        ({"loops": "one"}, "strategy.test_parameters.loops", "does not match the configured"),
        ({"loops": 1.5}, "strategy.test_parameters.loops", "does not match the configured"),
        ({"Loops": 1}, "strategy.test_parameters", "should match pattern"),
        ({"loops": [[1]]}, "strategy.test_parameters", "flat list of scalars"),
        ([1], "strategy.test_parameters", "Input should be an object"),
    ],
)
def test_check_refuses_malformed_test_parameters_at_their_source(raw, tests, field, message):
    with pytest.raises(ConfigurationError, match=message) as error:
        parsed(with_tests(raw, tests))
    assert error.value.field.startswith(field)
    assert error.value.line is not None


def test_editor_schema_accepts_test_parameters(raw):
    assert not list(validator("strategy.schema.json").iter_errors(with_tests(raw, {"loops": 1})))
    bad = with_tests(json.loads(json.dumps(raw)), {"Loops": 1})
    assert list(validator("strategy.schema.json").iter_errors(bad))


def source_with_tests(tmp_path, raw, tests):
    directory = source_workspace(tmp_path, raw)
    (directory / "config.yaml").write_text(yaml.safe_dump(with_tests(raw, tests), sort_keys=False))
    return directory


def started(tmp_path, monkeypatch, directory, param=None, production_parameters=False):
    """Start a test run against `directory` with a prepare that freezes the inputs."""

    def prepare(args, *, test_clock, overrides, fields):
        document, _ = author_inputs(
            Namespace(directory=directory), overrides=overrides, fields=fields
        )
        root = workspace(tmp_path)
        (root / "private").mkdir(exist_ok=True)
        inputs = document.inputs.model_dump(mode="json")
        (root / "private/author-inputs.json").write_text(json.dumps(inputs))
        return root

    monkeypatch.setattr("almanak.workspace.prepare", prepare)
    monkeypatch.setattr("almanak.workspace.launch", lambda root: Process())
    args = Namespace(
        directory=directory,
        config=None,
        output=tmp_path / "run",
        param=param,
        production_parameters=production_parameters,
    )
    verdict = testflow.empty_verdict()
    assert isinstance(testflow.start(args, args.output, verdict), Process)
    return verdict, json.loads((args.output / "private/author-inputs.json").read_text())


def test_test_run_applies_test_parameters_by_default(raw, tmp_path, monkeypatch):
    directory = source_with_tests(tmp_path, raw, {"loops": "1", "ltv": 0.5})
    before = (directory / "config.yaml").read_bytes()
    verdict, frozen = started(tmp_path, monkeypatch, directory, param=None)
    assert verdict["parameter_overrides"] == {"loops": 1, "ltv": 0.5}
    assert verdict["parameter_sources"] == {"loops": "test_parameters", "ltv": "test_parameters"}
    assert frozen["strategy"]["parameters"] == {**KNOBS, "loops": 1, "ltv": 0.5}
    assert "test_parameters" not in frozen["strategy"]
    assert (directory / "config.yaml").read_bytes() == before


def test_param_overrides_a_test_parameter(raw, tmp_path, monkeypatch):
    directory = source_with_tests(tmp_path, raw, {"loops": 1, "ltv": 0.5})
    verdict, frozen = started(tmp_path, monkeypatch, directory, param=["loops=3", "label=x"])
    assert verdict["parameter_overrides"] == {"loops": 3, "ltv": 0.5, "label": "x"}
    assert verdict["parameter_sources"] == {
        "loops": "--param",
        "ltv": "test_parameters",
        "label": "--param",
    }
    assert frozen["strategy"]["parameters"]["loops"] == 3


def test_production_parameters_skip_the_test_values(raw, tmp_path, monkeypatch):
    directory = source_with_tests(tmp_path, raw, {"loops": 1, "ltv": 0.5})
    verdict, frozen = started(
        tmp_path, monkeypatch, directory, param=None, production_parameters=True
    )
    assert verdict["parameter_overrides"] == verdict["parameter_sources"] == {}
    assert frozen["strategy"]["parameters"] == KNOBS
    verdict, frozen = started(
        tmp_path, monkeypatch, directory, param=["label=x"], production_parameters=True
    )
    assert verdict["parameter_overrides"] == {"label": "x"}
    assert verdict["parameter_sources"] == {"label": "--param"}
    assert frozen["strategy"]["parameters"]["loops"] == 2


def test_an_unknown_test_parameter_is_refused_as_the_admission(raw, tmp_path):
    directory = source_with_tests(tmp_path, raw, {"loop": 1})
    args = Namespace(
        directory=directory,
        config=None,
        output=tmp_path / "run",
        param=None,
        production_parameters=False,
    )
    verdict = testflow.empty_verdict()
    assert testflow.start(args, args.output, verdict) is None
    refusal = verdict["admission"]["refusal"]
    assert refusal["field"] == "strategy.test_parameters.loop"
    assert verdict["parameter_overrides"] == verdict["parameter_sources"] == {}
    assert not args.output.exists()
