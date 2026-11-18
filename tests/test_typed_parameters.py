"""A strategy's typed `strategy.parameters` model: validated before any hook, typed in each."""

import io
import json
from argparse import Namespace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
import yaml

from almanak.backtest.economic_model import parameters as economic_parameters
from almanak.backtest.economic_state import EconomicEvent, EconomicEventInput
from almanak.checks import author_inputs
from almanak.cli import error_document
from almanak.configuration.parameter_codec import hook_parameters
from almanak.configuration.preparation import check_preparation
from almanak.configuration.strategy_parameters import StrategyParameters, overrides
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.aave_v4.models import DecisionInput, OutcomeInput
from almanak.dashboard.evaluate import evaluate as dashboard
from almanak.effects import Effects
from almanak.runtime import (
    StrategyConfigRefused,
    strategy_decision,
    strategy_worker,
    validate_strategy_config,
)
from almanak.strategy import (
    DataDependencyRequest,
    EventRequest,
    OutcomeRequest,
    StrategyDefinition,
    ValidationRequest,
)
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.strategy_worker import evaluate_request, read_request
from almanak.triggers import IntervalSelection
from almanak.typed_parameters import ParametersRefused, TypedParameters, model_fields, typed
from tests.test_author_resolution import REFERENCE, parsed, resolve
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import raw as raw
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_strategy_parameters import started

MODEL = """
from decimal import Decimal as _Decimal

from pydantic import model_validator as _model_validator

from almanak.api import PositiveDecimal, TypedParameters, UnitFraction


class StrategyParams(TypedParameters):
    borrow_quantity: PositiveDecimal
    unwind_margin: PositiveDecimal = _Decimal("0.25")
    loops: int = 3
    fraction: UnitFraction = _Decimal("0.5")
    label: str = "wide"

    @_model_validator(mode="after")
    def bounded(self):
        if self.loops > 5:
            raise ValueError("loops must be at most 5")
        return self
"""
# Every hook asserts it sees the model's validated instance, then defers to the reference.
OBSERVING = """
from almanak.backtest.dependencies import DataDependencies as _DataDependencies

_validate_config, _capabilities, _decide, _on_outcome, _dashboard = (
    validate_config, capabilities, decide, on_outcome, dashboard
)


def seen(config):
    expected = StrategyParams(borrow_quantity=_Decimal("5"), loops=1)
    assert config.parameters == expected, config.parameters
    assert type(config.parameters.borrow_quantity) is _Decimal


def validate_config(config):
    seen(config)
    _validate_config(config)


def capabilities(config):
    seen(config)
    return _capabilities(config)


def data_dependencies(config):
    seen(config)
    return _DataDependencies()


def decide(context):
    seen(context.config)
    return _decide(context)


def on_outcome(context):
    seen(context.config)
    return _on_outcome(context)


def on_event(context):
    seen(context.config)
    return context.memory


def dashboard(ctx):
    seen(ctx.config)
    return _dashboard(ctx)
"""
KNOBS = {"borrow_quantity": "5", "loops": "1"}


def source(extra=OBSERVING):
    # The reference keeps its dashboard hook in dashboard.py; one source holds every hook here.
    reference = (
        (REFERENCE / "strategy.py").read_text() + "\n" + (REFERENCE / "dashboard.py").read_text()
    )
    return StrategyDefinition(filename="strategy.py", source=reference + MODEL + extra)


def with_knobs(raw, knobs=KNOBS):
    raw["strategy"]["parameters"] = knobs
    return raw


def namespace():
    result: dict = {}
    exec(compile(MODEL, "strategy.py", "exec"), result)
    return result


class Config:
    def __init__(self, parameters):
        self.parameters = parameters

    def model_copy(self, update):
        return Config(update["parameters"])


def checked(values):
    return typed(namespace(), Config(StrategyParameters(values))).parameters


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({"borrow_quantity": "5"}, {"borrow_quantity": Decimal(5), "loops": 3}),
        ({"borrow_quantity": 0.2, "loops": 1}, {"borrow_quantity": Decimal("0.2"), "loops": 1}),
        ({"borrow_quantity": 1, "loops": "2"}, {"borrow_quantity": Decimal(1), "loops": 2}),
        ({"borrow_quantity": 1, "loops": 4.0}, {"loops": 4}),
        ({"borrow_quantity": 1, "label": 3}, {"label": "3"}),
        ({"borrow_quantity": 1, "fraction": "0.25"}, {"fraction": Decimal("0.25")}),
    ],
)
def test_values_take_the_model_types_and_defaults(values, expected):
    model = checked(values)
    assert {key: getattr(model, key) for key in expected} == expected
    assert model.authored() == values
    with pytest.raises(ValueError, match="frozen"):
        model.loops = 2


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({}, "strategy.parameters.borrow_quantity: Field required"),
        ({"borrow_quantity": "-1"}, "strategy.parameters.borrow_quantity: Input should be greater"),
        (
            {"borrow_quantity": "Infinity"},
            "strategy.parameters.borrow_quantity: Input should be a ",
        ),
        ({"borrow_quantity": "nan"}, "strategy.parameters.borrow_quantity: Input should be a "),
        (
            {"borrow_quantity": 1, "loops": 1.5},
            "strategy.parameters.loops: Input should be a valid",
        ),
        (
            {"borrow_quantity": 1, "fraction": 1},
            "strategy.parameters.fraction: Input should be less",
        ),
        (
            {"borrow_quantity": 1, "loop": 2},
            "strategy.parameters.loop: is not a declared parameter",
        ),
        ({"borrow_quantity": 1, "loops": 6}, "strategy.parameters: loops must be at most 5"),
    ],
)
def test_a_refused_value_names_its_field(values, message):
    with pytest.raises(ParametersRefused) as refused:
        checked(values)
    assert str(refused.value).startswith(message)
    assert message.startswith(refused.value.field)


def test_a_strategy_without_a_model_keeps_the_mapping():
    parameters = StrategyParameters({"loops": 1})
    config = Config(parameters)
    assert typed({"TypedParameters": TypedParameters}, config) is config
    second = {**namespace(), "Other": type("Other", (namespace()["StrategyParams"],), {})}
    with pytest.raises(TypeError, match="several unrelated ones"):
        typed(second, config)


@pytest.mark.parametrize(
    ("text", "fields"),
    [
        (MODEL, {"borrow_quantity", "unwind_margin", "loops", "fraction", "label"}),
        ("import almanak.api as api\nclass K(api.TypedParameters): ...\n", set()),
        ("from almanak.api import TypedParameters as P\nclass K(P): ...\n", set()),
        (
            "from typing import ClassVar\nfrom almanak.api import TypedParameters\n"
            "class Base(TypedParameters):\n    size: int = 1\n    _hidden: int = 0\n"
            "    LIMIT: ClassVar[int] = 3\n"
            "class StrategyParams(Base):\n    loops: int = 2\n",
            {"size", "loops"},
        ),
        ("class K: ...\n", None),
        ("def broken(:\n", None),
        (None, None),
    ],
)
def test_the_model_fields_are_read_without_running_the_source(tmp_path, text, fields):
    path = tmp_path / "strategy.py"
    if text is not None:
        path.write_text(text)
    assert model_fields(path) == fields


def test_every_hook_sees_the_validated_model(raw, setup, recorded):
    document = parsed(with_knobs(raw))
    report = check_preparation(document, source(), resolve(document, setup))
    states = {s["stage"]: s["status"] for s in report["stages"]}
    assert states["strategy"] == states["capabilities"] == "passed"

    config = recorded.config.model_copy(update={"parameters": StrategyParameters(KNOBS)})
    request = DataDependencyRequest(strategy=source(), config=hook_parameters(config))
    assert json.loads(strategy_worker(request)) is not None
    context = DecisionInput(
        config=config,
        memory=Memory(),
        owned=recorded.owned,
        observations=recorded.baseline,
        trigger=recorded.outcomes[0].decision.trigger,
    )
    allocation = config.allocations[0]
    assert strategy_decision(source(), context).intent == config.supply(
        allocation.name, allocation.capital
    )
    outcome = OutcomeInput(
        config=config, memory=recorded.memory, result=recorded.outcomes[0].strategy_result()
    )
    strategy_worker(OutcomeRequest(strategy=source(), context=outcome))
    state = recorded.model_copy(update={"strategy": source(), "config": config})
    assert dashboard(state, captured_at=42).status == "ready"


def test_on_event_sees_the_model_in_backtests(recorded):
    config = recorded.config.model_copy(update={"parameters": StrategyParameters(KNOBS)})
    author = hook_parameters(config)
    config = economic_parameters(
        None, author, IntervalSelection(configured=author.decision_trigger)
    )
    request = EventRequest(
        strategy=source(),
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
    assert Memory.model_validate_json(strategy_worker(request)).schema_id


def test_a_refused_value_is_invalid_config_before_validate_config(monkeypatch, capsys, recorded):
    definition = source("\ndef validate_config(config):\n    raise AssertionError\n")
    config = recorded.config.model_copy(
        update={"parameters": StrategyParameters({"borrow_quantity": "0", "extra": 1})}
    )
    request = ValidationRequest(strategy=definition, config=config)
    monkeypatch.setattr("sys.argv", ["worker", "--validate-config"])
    monkeypatch.setattr("sys.stdin", io.StringIO(request.model_dump_json()))
    evaluate_request(*read_request())
    assert json.loads(capsys.readouterr().out) == {
        "refusal": "invalid_config",
        "message": "strategy.parameters.extra: is not a declared parameter; "
        "strategy.parameters.borrow_quantity: Input should be greater than 0",
        "line": None,
        "field": "strategy.parameters.extra",
    }
    with pytest.raises(StrategyConfigRefused) as refused:
        validate_strategy_config(definition, config)
    document = error_document(refused.value)
    assert document is not None
    assert {k: document[k] for k in ("error", "code", "field")} == {
        "error": "invalid_config",
        "code": "strategy_config_refused",
        "field": "strategy.parameters.extra",
    }


def source_workspace(tmp_path, raw, knobs, tests=None, model=True):
    directory = tmp_path / "source"
    directory.mkdir(exist_ok=True)
    raw = with_knobs(raw, knobs)
    if tests:
        raw["strategy"]["test_parameters"] = tests
    (directory / "config.yaml").write_text(yaml.safe_dump(raw, sort_keys=False))
    (directory / "strategy.py").write_text(
        source("").source if model else "def decide(context):\n    return None\n"
    )
    return directory


def test_param_keeps_the_yaml_scalar_for_the_model_to_convert(raw, tmp_path):
    directory = source_workspace(tmp_path, raw, {"borrow_quantity": 5, "loops": 2})
    document, _ = author_inputs(
        Namespace(directory=directory), overrides=overrides(["borrow_quantity=0.5", 'loops="1"'])
    )
    assert dict(document.inputs.strategy.parameters) == {"borrow_quantity": 0.5, "loops": "1"}
    without = source_workspace(tmp_path, raw, {"borrow_quantity": 5}, model=False)
    with pytest.raises(ConfigurationError, match="does not match the configured int"):
        author_inputs(Namespace(directory=without), overrides=overrides(["borrow_quantity=0.5"]))


def test_test_parameters_are_validated_against_the_model(raw, setup, tmp_path, monkeypatch):
    from almanak import testflow

    tests = {"borrow_quantity": "0.5", "loops": 9}
    directory = source_workspace(tmp_path, raw, {"borrow_quantity": 5, "loops": 2}, tests)

    def prepare(args, *, test_clock, overrides, fields):
        document, definition = author_inputs(
            Namespace(directory=directory), overrides=overrides, fields=fields
        )
        assert dict(document.inputs.strategy.parameters) == tests
        check_preparation(document, definition, resolve(document, setup))
        raise AssertionError("the model must refuse the test values")

    monkeypatch.setattr("almanak.workspace.prepare", prepare)
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
    assert refusal["error"] == "invalid_config"
    assert refusal["field"] == "strategy.parameters"
    assert "strategy.parameters: loops must be at most 5" in refusal["message"]


def test_a_typed_config_dumps_as_authored(recorded):
    config = recorded.config.model_copy(update={"parameters": StrategyParameters(KNOBS)})
    bound = typed(namespace(), config)
    assert bound.parameters.loops == 1
    assert bound.model_dump(mode="json") == config.model_dump(mode="json")
    assert type(bound).model_validate(bound).parameters is bound.parameters


def test_another_model_instance_is_revalidated_from_its_authored_values():
    other = checked({"borrow_quantity": "5", "loops": "1"})
    again = typed(namespace(), Config(other)).parameters
    assert type(again) is not type(other) and again.model_dump() == other.model_dump()
    assert again.authored() == {"borrow_quantity": "5", "loops": "1"}


def test_param_sets_a_model_field_config_yaml_leaves_to_its_default(raw, tmp_path, monkeypatch):
    from almanak.configuration.unresolved import read_inputs

    directory = source_workspace(tmp_path, raw, {"borrow_quantity": "5"})
    before = (directory / "config.yaml").read_bytes()
    digest = read_inputs(directory / "config.yaml").inputs.digest
    verdict, frozen = started(tmp_path, monkeypatch, directory, param=["loops=1"])
    assert verdict["parameter_overrides"] == {"loops": 1}
    assert verdict["parameter_sources"] == {"loops": "--param"}
    assert frozen["strategy"]["parameters"] == {"borrow_quantity": "5", "loops": 1}
    # The source keeps its bytes and its digest: the override is in the fork copy only.
    assert (directory / "config.yaml").read_bytes() == before
    assert read_inputs(directory / "config.yaml").inputs.digest == digest


def test_test_parameters_set_a_defaulted_model_field(raw, tmp_path, monkeypatch):
    from almanak.configuration.unresolved import read_inputs

    directory = source_workspace(tmp_path, raw, {"borrow_quantity": "5"}, {"loops": 1})
    verdict, frozen = started(tmp_path, monkeypatch, directory, param=None)
    assert verdict["parameter_overrides"] == {"loops": 1}
    assert verdict["parameter_sources"] == {"loops": "test_parameters"}
    assert frozen["strategy"]["parameters"] == {"borrow_quantity": "5", "loops": 1}
    assert "test_parameters" not in frozen["strategy"]
    # Without the fork test, the parsed inputs never carry the test value.
    fields = model_fields(directory / "strategy.py")
    parsed_inputs = read_inputs(directory / "config.yaml", fields=fields).inputs
    assert dict(parsed_inputs.strategy.parameters) == {"borrow_quantity": "5"}


def test_a_key_the_model_does_not_declare_is_refused_listing_its_fields(raw, tmp_path):
    directory = source_workspace(tmp_path, raw, {"borrow_quantity": "5"})
    with pytest.raises(ConfigurationError) as refused:
        author_inputs(Namespace(directory=directory), overrides=overrides(["loop=1"]))
    assert refused.value.field == "strategy.parameters.loop"
    assert refused.value.code == "unknown_parameter_override"
    assert str(refused.value).startswith(
        "--param 'loop' is not a field of the strategy's TypedParameters model. Model fields: "
        "borrow_quantity, fraction, label, loops, unwind_margin."
    )
    tests = source_workspace(tmp_path, raw, {"borrow_quantity": "5"}, {"loop": 1})
    with pytest.raises(ConfigurationError, match="Model fields: borrow_quantity") as refused:
        author_inputs(Namespace(directory=tests))
    assert refused.value.field == "strategy.test_parameters.loop"
    assert refused.value.code == "unknown_test_parameter"


def test_without_a_model_an_override_key_must_be_in_config_yaml(raw, tmp_path):
    directory = source_workspace(tmp_path, raw, {"borrow_quantity": "5"}, model=False)
    with pytest.raises(ConfigurationError, match="Configured parameters: borrow_quantity"):
        author_inputs(Namespace(directory=directory), overrides=overrides(["loops=1"]))
    tests = source_workspace(tmp_path, raw, {"borrow_quantity": "5"}, {"loops": 1}, model=False)
    with pytest.raises(ConfigurationError, match="is not in strategy.parameters"):
        author_inputs(Namespace(directory=tests))


@pytest.mark.parametrize(
    ("pair", "field", "reason"),
    [
        ("loops=1.5", "strategy.parameters.loops", "valid integer"),
        ("fraction=1", "strategy.parameters.fraction", "less than 1"),
        ("borrow_quantity=0", "strategy.parameters.borrow_quantity", "greater than 0"),
    ],
)
def test_an_override_the_model_refuses_is_invalid_config_naming_the_field(
    raw, setup, tmp_path, monkeypatch, pair, field, reason
):
    from almanak import testflow

    directory = source_workspace(tmp_path, raw, {"borrow_quantity": "5"})

    def prepare(args, *, test_clock, overrides, fields):
        document, definition = author_inputs(
            Namespace(directory=directory), overrides=overrides, fields=fields
        )
        check_preparation(document, definition, resolve(document, setup))
        raise AssertionError("the model must refuse the override")

    monkeypatch.setattr("almanak.workspace.prepare", prepare)
    args = Namespace(
        directory=directory,
        config=None,
        output=tmp_path / "run",
        param=[pair],
        production_parameters=False,
    )
    verdict = testflow.empty_verdict()
    assert testflow.start(args, args.output, verdict) is None
    refusal = verdict["admission"]["refusal"]
    assert (refusal["error"], refusal["code"], refusal["field"]) == (
        "invalid_config",
        "strategy_config_refused",
        field,
    )
    assert refusal["message"].startswith(f"{field}: ") and reason in refusal["message"]


SHARED = """
from pydantic import Field

from almanak.api import PositiveDecimal, TypedParameters


class SharedParams(TypedParameters):
    borrow_quantity: PositiveDecimal
    loops: int = Field(default=2, ge=1)


class StrategyParams(SharedParams):
    label: str = "wide"
"""


def test_a_model_extending_a_shared_base_is_the_one_selected():
    namespace: dict = {}
    exec(compile(SHARED, "strategy.py", "exec"), namespace)
    selected = typed(namespace, Config(StrategyParameters({"borrow_quantity": "5"}))).parameters
    assert type(selected).__name__ == "StrategyParams"
    assert (selected.loops, selected.label) == (2, "wide")
    unrelated = {**namespace, "Other": type("Other", (TypedParameters,), {})}
    with pytest.raises(TypeError, match="Other, StrategyParams"):
        typed(unrelated, Config(StrategyParameters({"borrow_quantity": "5"})))


def test_override_fields_match_the_model_the_runtime_selects(raw, tmp_path):
    path = tmp_path / "strategy.py"
    path.write_text(SHARED)
    assert model_fields(path) == {"borrow_quantity", "loops", "label"}
    # A base imported from outside the SDK is resolved by loading the strategy in the sandbox.
    path.write_text(
        SHARED + "\nfrom yaml import YAMLObject\n\n\nclass Tag(YAMLObject):\n    pass\n"
    )
    assert model_fields(path) == {"borrow_quantity", "loops", "label"}
    directory = source_workspace(tmp_path, raw, {"borrow_quantity": "5"})
    (directory / "strategy.py").write_text(SHARED)
    document, _ = author_inputs(Namespace(directory=directory), overrides=overrides(["loops=1"]))
    assert dict(document.inputs.strategy.parameters) == {"borrow_quantity": "5", "loops": 1}
    with pytest.raises(ConfigurationError, match="Model fields: borrow_quantity, label, loops"):
        author_inputs(Namespace(directory=directory), overrides=overrides(["fraction=0.5"]))


def compiled(text):
    result: dict = {}
    exec(
        compile(
            "from pydantic import Field\nfrom almanak.api import TypedParameters\n" + text,
            "strategy.py",
            "exec",
        ),
        result,
    )
    return result


@pytest.mark.parametrize(
    "declaration",
    [
        'loop_count: int = Field(default=3, alias="loops")',
        'loop_count: int = Field(default=3, validation_alias="loops")',
    ],
)
def test_a_field_with_an_alias_is_refused_naming_it(declaration):
    namespace = compiled(f"class StrategyParams(TypedParameters):\n    {declaration}\n")
    for values in ({"loops": 1}, {"loop_count": 1}, {}):
        with pytest.raises(ParametersRefused, match="declares the alias 'loops'") as refused:
            typed(namespace, Config(StrategyParameters(values)))
        assert refused.value.field == "strategy.parameters.loop_count"


def test_an_alias_generator_is_refused():
    namespace = compiled(
        "from pydantic import ConfigDict\nfrom pydantic.alias_generators import to_camel\n"
        "class StrategyParams(TypedParameters):\n"
        "    model_config = ConfigDict(alias_generator=to_camel)\n    loop_count: int = 3\n"
    )
    with pytest.raises(ParametersRefused, match="alias 'loopCount'"):
        typed(namespace, Config(StrategyParameters({})))


@pytest.mark.parametrize(
    "annotation", ["list[int]", "list[int] | None", "set[str]", "dict[str, int]"]
)
def test_a_mutable_collection_field_is_refused(annotation):
    namespace = compiled(
        f"class StrategyParams(TypedParameters):\n    amounts: {annotation} = None\n"
    )
    with pytest.raises(ParametersRefused, match="declare a list as tuple") as refused:
        typed(namespace, Config(StrategyParameters({"amounts": (1, 2)})))
    assert refused.value.field == "strategy.parameters.amounts"


def test_a_tuple_field_cannot_be_changed_by_a_hook():
    namespace = compiled("class StrategyParams(TypedParameters):\n    amounts: tuple[int, ...]\n")
    model = typed(namespace, Config(StrategyParameters({"amounts": ("1", 2)}))).parameters
    assert model.amounts == (1, 2)
    with pytest.raises(AttributeError):
        model.amounts.append(3)  # type: ignore[attr-defined]
    assert model.authored() == {"amounts": ("1", 2)}


def test_a_refused_value_is_invalid_config_from_data_dependencies_and_backtests(recorded):
    from almanak.backtest.strategy_prices import declared_prices

    config = recorded.config.model_copy(
        update={"parameters": StrategyParameters({"borrow_quantity": "0"})}
    )
    request = DataDependencyRequest(strategy=source(), config=hook_parameters(config))
    for action in (
        lambda: strategy_worker(request),
        lambda: declared_prices(source(), hook_parameters(config), economic=False),
    ):
        with pytest.raises(StrategyConfigRefused) as refused:
            action()
        document = error_document(refused.value)
        assert document is not None
        assert (document["error"], document["field"]) == (
            "invalid_config",
            "strategy.parameters.borrow_quantity",
        )
        assert str(refused.value).startswith(
            "strategy.parameters.borrow_quantity: Input should be greater than 0"
        )


V2_IMPORT = """
from almanak.framework.strategies import IntentStrategy


class LegacyStrategy(IntentStrategy):
    pass
"""


def test_a_missing_sdk_module_is_refused_by_the_strategy_stage(raw, setup, tmp_path, monkeypatch):
    from almanak import testflow

    path = tmp_path / "strategy.py"
    for text in (V2_IMPORT, V2_IMPORT + MODEL):
        path.write_text(text)
        assert model_fields(path) is None
    directory = source_workspace(tmp_path, raw, {"borrow_quantity": "5"})
    (directory / "strategy.py").write_text(source("").source + V2_IMPORT)

    def prepare(args, *, test_clock, overrides, fields):
        assert fields is None
        document, definition = author_inputs(Namespace(directory=directory), fields=fields)
        check_preparation(document, definition, resolve(document, setup))
        raise AssertionError("the strategy stage must refuse the import")

    monkeypatch.setattr("almanak.workspace.prepare", prepare)
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
    assert refusal["field"] == "strategy"
    assert "No module named 'almanak.framework'" in refusal["message"]


HIDDEN = "\nfrom almanak.api import TypedParameters as _Typed\n"
MIXIN = (
    HIDDEN
    + """

class Knobs:
    extra_knob: int = 1


class StrategyParams(Knobs, _Typed):
    borrow_quantity: str
"""
)


@pytest.mark.parametrize(
    "text",
    [
        "if True:\n    class StrategyParams(_Typed):\n        borrow_quantity: str\n",
        "try:\n    class StrategyParams(_Typed):\n        borrow_quantity: str\n"
        "except ImportError:\n    pass\n",
        "StrategyParams = type('StrategyParams', (_Typed,), "
        "{'__annotations__': {'borrow_quantity': str, 'extra_knob': int}, 'extra_knob': 1})\n",
        MIXIN.removeprefix(HIDDEN),
    ],
)
def test_a_model_the_source_cannot_show_is_read_from_the_loaded_strategy(raw, tmp_path, text):
    directory = source_workspace(tmp_path, raw, {"borrow_quantity": "5"}, model=False)
    path = directory / "strategy.py"
    path.write_text(path.read_text() + HIDDEN + text)
    fields = model_fields(path)
    assert fields is not None and {"borrow_quantity"} <= fields
    if "extra_knob" in fields:
        document, _ = author_inputs(
            Namespace(directory=directory), overrides=overrides(["extra_knob=7"])
        )
        assert dict(document.inputs.strategy.parameters) == {
            "borrow_quantity": "5",
            "extra_knob": 7,
        }


def test_a_plain_mixin_field_is_a_model_field():
    model = typed(
        compiled(MIXIN), Config(StrategyParameters({"borrow_quantity": 5, "extra_knob": "7"}))
    )
    assert (model.parameters.borrow_quantity, model.parameters.extra_knob) == ("5", 7)


SCALARS = """
class StrategyParams(TypedParameters):
    loops: int = 1
    rate: float = 0.5
    size: Decimal = Decimal(1)
    dry_run: bool = False
    flag: bool | None = None
    cap: int | None = None
    mixed: int | str = 0
"""


def scalars(values):
    namespace = compiled("from decimal import Decimal\n" + SCALARS)
    return typed(namespace, Config(StrategyParameters(values))).parameters


@pytest.mark.parametrize(
    ("values", "field"),
    [
        ({"loops": True}, "loops"),
        ({"rate": False}, "rate"),
        ({"size": True}, "size"),
        ({"cap": True}, "cap"),
        ({"loops": "1_000"}, "loops"),
        ({"rate": "1_0.5"}, "rate"),
        ({"dry_run": "yes"}, "dry_run"),
        ({"dry_run": 1}, "dry_run"),
        ({"flag": "1"}, "flag"),
    ],
)
def test_a_typed_field_refuses_what_an_untyped_knob_refuses(values, field):
    with pytest.raises(ParametersRefused) as refused:
        scalars(values)
    assert refused.value.field == f"strategy.parameters.{field}"


def test_a_typed_field_takes_what_an_untyped_knob_takes():
    model = scalars(
        {"dry_run": " True ", "flag": None, "cap": None, "rate": "1e-5", "loops": "2", "mixed": "x"}
    )
    assert (model.dry_run, model.flag, model.cap, model.rate, model.loops, model.mixed) == (
        True,
        None,
        None,
        1e-05,
        2,
        "x",
    )
    assert scalars({"flag": "false", "size": "1e-18"}).size == Decimal("1e-18")


def test_the_capability_worker_reports_a_refused_value(monkeypatch, capsys):
    from almanak import capability_worker
    from tests.capability_support import transfer_codec

    class Codec:
        def execute_hook(self, request):
            raise ParametersRefused(
                "strategy.parameters.loops: too many", "strategy.parameters.loops"
            )

    monkeypatch.setattr(capability_worker, "restrict_author", lambda: None)
    monkeypatch.setattr(
        capability_worker, "import_module", lambda name: SimpleNamespace(Codec=Codec)
    )
    monkeypatch.setattr("sys.argv", ["capability_worker", "fixture:Codec"])
    monkeypatch.setattr("sys.stdin", io.StringIO("{}"))
    capability_worker.main()
    output = capsys.readouterr().out
    monkeypatch.setattr("almanak.worker_process.run_worker", lambda *args: output)
    config = SimpleNamespace(model_dump=lambda mode: {})
    with pytest.raises(StrategyConfigRefused) as refused:
        transfer_codec().evaluate(StrategyDefinition(filename="strategy.py", source=""), config)
    document = error_document(refused.value)
    assert document is not None
    assert (document["error"], document["field"]) == ("invalid_config", "strategy.parameters.loops")
    assert str(refused.value) == "strategy.parameters.loops: too many"
