"""An author's validate_config refusal is a final `invalid_config`; a crash stays a traceback."""

import io
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import testflow
from almanak.checks import CheckFailure, check_validation, checked
from almanak.cli import error_document
from almanak.configuration.parameter_codec import hook_parameters
from almanak.runtime import StrategyConfigRefused, validate_strategy_config, validation_result
from almanak.strategy import StrategyDefinition, ValidationRequest
from almanak.strategy_worker import evaluate_request, read_request
from almanak.supervisor import Supervisor
from almanak.workspace import FINAL_FAILURES, next_commands

HELPERS = """\
from almanak.api import InvalidConfig


def require(ok, message):
    if not ok:
        raise InvalidConfig(message)


def check(config):
    raise ValueError("helper refusal")


"""


def strategy(body):
    return StrategyDefinition(
        filename="strategy.py", source=HELPERS + "def validate_config(config):\n" + body
    )


def evaluate(monkeypatch, definition, config):
    request = ValidationRequest(strategy=definition, config=config)
    monkeypatch.setattr("sys.argv", ["worker", "--validate-config"])
    monkeypatch.setattr("sys.stdin", io.StringIO(request.model_dump_json()))
    evaluate_request(*read_request())


@pytest.mark.parametrize(
    ("body", "message", "line"),
    [
        ('    raise ValueError("floor below minimum")\n', "floor below minimum", 14),
        ('    raise ValueError(\n        "floor " + "below"\n    )\n', "floor below", 14),
        ('    require(False, "band too narrow")\n', "band too narrow", 6),
        ('    raise InvalidConfig("cap exceeded")\n', "cap exceeded", 14),
        ('    raise ValueError("")\n', "validate_config refused the configuration", 14),
    ],
)
def test_a_raised_refusal_prints_its_message_and_line(
    monkeypatch, capsys, recorded, body, message, line
):
    evaluate(monkeypatch, strategy(body), recorded.config)
    document = json.loads(capsys.readouterr().out)
    assert document == {"refusal": "invalid_config", "message": message, "line": line}


@pytest.mark.parametrize(
    ("body", "error"),
    [
        ("    low, high, step, extra = (1, 2, 3)\n", ValueError),
        ('    int("x")\n', ValueError),
        ("    raise ValueError(f\"{int('x')}\")\n", ValueError),
        ("    check(config)\n", ValueError),
        ("    config.allocations = ()\n", ValueError),
        ("    config.allocations[0].reserve.asset.decimals\n", AttributeError),
        (
            "    try:\n        a, b = (1,)\n    except ValueError:\n        raise\n",
            ValueError,
        ),
    ],
)
def test_other_errors_stay_crashes(monkeypatch, recorded, body, error):
    with pytest.raises(error) as raised:
        evaluate(monkeypatch, strategy(body), recorded.config)
    assert type(raised.value) is not StrategyConfigRefused


@pytest.mark.parametrize(
    "output", ["false", '{"refusal": "other"}', '{"refusal": "invalid_config"}']
)
def test_an_unexpected_validation_output_is_still_refused(output):
    with pytest.raises(RuntimeError, match="invalid response"):
        validation_result(output, strategy("    pass\n"))


@pytest.mark.parametrize("preloaded", [False, True])
def test_a_refusal_is_invalid_config_at_check_and_at_runtime(recorded, preloaded):
    from almanak.worker_session import preloaded_workers

    definition = strategy('    raise ValueError("unwind_health_factor must exceed the floor")\n')
    parameters = hook_parameters(recorded.config)
    with preloaded_workers() if preloaded else nullcontext():
        with pytest.raises(StrategyConfigRefused) as runtime:
            validate_strategy_config(definition, recorded.config)
        stages: list = []
        with pytest.raises(CheckFailure) as check:
            checked(
                stages, "strategy", "strategy.py", lambda: check_validation(parameters, definition)
            )
    assert runtime.value.summary() == (
        "the strategy refused the configuration (strategy.py:14): "
        "unwind_health_factor must exceed the floor"
    )
    document = error_document(check.value)
    assert document is not None
    assert {k: document[k] for k in ("error", "code", "field", "line", "message")} == {
        "error": "invalid_config",
        "code": "strategy_config_refused",
        "field": "strategy.parameters",
        "line": 14,
        "message": "unwind_health_factor must exceed the floor",
    }
    assert stages[-1]["status"] == "failed"


@pytest.mark.parametrize("preloaded", [False, True])
def test_a_crash_reports_the_traceback_from_the_strategy_frame(recorded, preloaded):
    from almanak.worker_session import preloaded_workers

    definition = strategy("    config.allocations[0].reserve.asset.decimals\n")
    with preloaded_workers() if preloaded else nullcontext():
        with pytest.raises(RuntimeError) as crash:
            validate_strategy_config(definition, recorded.config)
    message = str(crash.value)
    assert message.startswith("strategy worker failed: Traceback (most recent call last):\n")
    assert 'File "strategy.py", line 14, in validate_config' in message
    assert "config.allocations[0].reserve.asset.decimals" in message
    assert "AttributeError" in message
    for sdk in ("strategy_worker.py", "worker_server.py"):
        assert sdk not in message


def setup_failure(raised):
    persisted: list = []
    host = SimpleNamespace(
        password="x",
        setup_active=True,
        state={"stage": "provisioning"},
        persist=lambda **changes: persisted.append(changes),
    )

    def work():
        raise raised

    Supervisor.setup_worker(host, work)
    [changes] = persisted
    return changes


def wrapped_refusal():
    refused = StrategyConfigRefused("floor below minimum", "strategy.py", 14)
    try:
        raise CheckFailure(refused, [], "strategy.py") from refused
    except CheckFailure as wrapped:
        return wrapped


@pytest.mark.parametrize(
    "raised", [StrategyConfigRefused("floor below minimum", "strategy.py", 14), wrapped_refusal()]
)
def test_setup_marks_a_refusal_final_not_a_retryable_start(raised):
    changes = setup_failure(raised)
    assert changes["failure"] == "invalid_config" and changes["cause"] is None
    assert changes["diagnostic"] == (
        "the strategy refused the configuration (strategy.py:14): floor below minimum"
    )
    assert "invalid_config" in FINAL_FAILURES
    state = {"stage": "failed", "failure": "invalid_config"}
    assert next_commands(Path("/run"), state) == ["almanak shutdown --run /run"]
    assert setup_failure(RuntimeError("strategy worker failed: boom"))["failure"] == "start"


def test_test_run_admission_reports_the_refusal_as_strategy_invalid_config(monkeypatch):
    failed = {
        "stage": "failed",
        "failure": "invalid_config",
        "diagnostic": "the strategy refused the configuration (strategy.py:14): floor",
    }
    monkeypatch.setattr(testflow, "supervisor_status", lambda root: failed)
    verdict = testflow.empty_verdict()
    process = SimpleNamespace(poll=lambda: None)
    assert testflow.admitted(Path("."), process, verdict) is False
    refusal = verdict["admission"]["refusal"]
    assert refusal == {
        "stage": "failed",
        "message": failed["diagnostic"],
        "error": "invalid_config",
        "kind": "invalid_config",
        "class": "strategy",
        "transient": False,
    }


def test_a_crash_traceback_is_trimmed_in_every_chained_exception():
    from almanak.strategy_refusal import author_traceback, register_source

    definition = strategy(
        "    try:\n        config.missing\n    except AttributeError as exc:\n"
        "        raise TypeError('wrapped') from exc\n"
    )
    register_source(definition)
    namespace: dict = {}
    exec(compile(definition.source, definition.filename, "exec"), namespace)
    try:
        namespace["validate_config"](SimpleNamespace())
    except TypeError as exc:
        report = author_traceback(exc, "strategy.py")
    assert __file__ not in report
    assert report.count('File "strategy.py"') == 2
    assert "config.missing" in report and "raise TypeError('wrapped') from exc" in report
    assert report.rstrip().endswith("TypeError: wrapped")
    try:
        raise RuntimeError("sdk contract")
    except RuntimeError as exc:
        assert __file__ in author_traceback(exc, "strategy.py")
