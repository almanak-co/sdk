"""Component checks; these do not substitute for the macOS sandbox proof."""

import io
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v4.models import LendingConfig
from almanak.runtime import Runtime, StrategyConfigRefused, validate_strategy_config
from almanak.strategy import StrategyDefinition, ValidationRequest
from almanak.strategy_worker import evaluate_request, read_request
from almanak.worker_session import preloaded_workers


def invoke(monkeypatch, definition, config):
    request = ValidationRequest(strategy=definition, config=config)
    monkeypatch.setattr("sys.argv", ["worker", "--validate-config"])
    monkeypatch.setattr("sys.stdin", io.StringIO(request.model_dump_json()))
    evaluate_request(*read_request())


REFUSED = '{"refusal": "invalid_config", "message": "invalid sizing", "line": 1}\n'


@pytest.mark.parametrize(
    "source,error,output",
    [
        ("def decide(context): return None", None, "null\n"),
        ("def validate_config(config): assert config.allocations[0].capital > 0", None, "null\n"),
        ('def validate_config(config): raise ValueError("invalid sizing")', None, REFUSED),
        ("def validate_config(config): return False", TypeError, None),
        ("validate_config = None", TypeError, None),
        ("def validate_config(config): config.allocations = ()", ValueError, None),
    ],
)
def test_optional_hook_contract(monkeypatch, capsys, recorded, source, error, output):
    definition = StrategyDefinition(filename="strategy.py", source=source)
    if error:
        with pytest.raises(error):
            invoke(monkeypatch, definition, recorded.config)
    else:
        invoke(monkeypatch, definition, recorded.config)
        assert capsys.readouterr().out == output


@pytest.mark.parametrize("wrong_asset", [False, True])
def test_reference_hook(monkeypatch, capsys, recorded, reference_strategy, wrong_asset):
    raw = recorded.config.model_dump(mode="json")
    if wrong_asset:
        raw["allocations"][0]["reserve"]["asset"] = "eip155:31337/erc20:0x" + "11" * 20
    import json

    config = LendingConfig.model_validate_json(json.dumps(raw))
    invoke(monkeypatch, reference_strategy, config)
    output = capsys.readouterr().out
    if wrong_asset:
        refusal = json.loads(output)
        assert refusal["refusal"] == "invalid_config" and "WETH reserve" in refusal["message"]
    else:
        assert output == "null\n"


@pytest.mark.parametrize("creating,reject", [(True, False), (True, True), (False, True)])
def test_validation_precedes_creation_and_is_not_repeated_on_resume(
    monkeypatch,
    tmp_path,
    recorded,
    reference_strategy,
    creating,
    reject,
):
    events = []
    stored = recorded.model_copy(update={"strategy": reference_strategy})
    gateway = SimpleNamespace(
        load=lambda: stored,
        create=lambda *args: events.append("create"),
        acquire=lambda: events.append("acquire"),
        audit_history=lambda state: events.append("audit"),
        close=lambda: events.append("close"),
    )

    def validate(definition, config):
        assert definition == reference_strategy
        assert config is recorded.config
        events.append("validate")
        if reject:
            raise ValueError("invalid sizing")

    monkeypatch.setattr("almanak.runtime.worker_command", lambda **kwargs: [])
    monkeypatch.setattr("almanak.runtime.GatewayClient", lambda *args: gateway)
    monkeypatch.setattr("almanak.runtime.validate_strategy_config", validate)
    kwargs = {"create": recorded.config, "strategy": reference_strategy} if creating else {}
    if creating and reject:
        with pytest.raises(ValueError, match="invalid sizing"):
            Runtime("unused", tmp_path / "auth", **kwargs)
        assert events == ["validate", "close"]
    else:
        runner = Runtime("unused", tmp_path / "auth", **kwargs)
        runner.close()
        prefix = ["validate", "create"] if creating else []
        assert events == prefix + ["acquire", "audit", "close"]


@pytest.mark.parametrize("returncode,stdout", [(0, "null"), (0, "false"), (1, "")])
def test_validation_transport(monkeypatch, recorded, reference_strategy, returncode, stdout):
    def run(module, body, arguments):
        assert module == "almanak.strategy_worker" and arguments == ["--validate-config"]
        from almanak.configuration.parameter_codec import hook_parameters

        parameters = hook_parameters(recorded.config)
        request = ValidationRequest[type(parameters)].model_validate_json(body)
        assert request.config == parameters
        if returncode:
            raise RuntimeError("hook failed")
        return stdout

    monkeypatch.setattr("almanak.runtime.run_worker", run)
    if returncode or stdout != "null":
        with pytest.raises(RuntimeError):
            validate_strategy_config(reference_strategy, recorded.config)
    else:
        validate_strategy_config(reference_strategy, recorded.config)


def test_resolved_validation_preserves_parameters_and_excludes_execution(recorded):
    from almanak.configuration.parameter_codec import hook_parameters
    from almanak.connectors.aave_v4.models import LendingParameters

    parameters = hook_parameters(recorded.config)
    assert type(parameters) is LendingParameters
    assert hook_parameters(parameters) is parameters
    for field in LendingParameters.model_fields:
        assert getattr(parameters, field) == getattr(recorded.config, field)
    assert not hasattr(parameters, "production")
    assert not hasattr(parameters, "fork_bindings")
    assert not hasattr(parameters, "preparation_digest")


def test_validation_refuses_unresolved_or_incomplete_parameter_objects():
    from almanak.configuration.parameter_codec import hook_parameters
    from tests.test_composed_configuration import parameters

    for config in (parameters(), SimpleNamespace(connector="aave_v4")):
        with pytest.raises(ValueError, match="requires resolved connector parameters"):
            hook_parameters(config)


@pytest.mark.parametrize("preloaded", [False, True])
def test_economic_validation_retains_its_resolved_model(recorded, preloaded):
    from almanak.backtest.economic_state import EconomicParameters
    from almanak.configuration.parameter_codec import hook_parameters

    parameters = EconomicParameters.model_validate(hook_parameters(recorded.config).model_dump())
    assert hook_parameters(parameters) is parameters
    definition = StrategyDefinition(
        filename="strategy.py",
        source=(
            "def validate_config(config):\n"
            "    assert type(config).__name__ == 'EconomicParameters'\n"
            "    assert config.accounts.bindings\n"
            "    assert not hasattr(config, 'production')\n"
        ),
    )
    with preloaded_workers() if preloaded else nullcontext():
        validate_strategy_config(definition, parameters, economic=True)


def test_public_check_and_economic_validation_share_resolved_author_fields(recorded):
    from almanak.backtest.economic_state import EconomicParameters
    from almanak.checks import check_validation
    from almanak.configuration.parameter_codec import hook_parameters

    parameters = hook_parameters(recorded.config)
    economic = EconomicParameters.model_validate(parameters.model_dump())
    definition = StrategyDefinition(
        filename="strategy.py",
        source=(
            "import json\n"
            "def validate_config(config):\n"
            "    assert config.model_dump(mode='json') == "
            f"json.loads({parameters.model_dump_json()!r})\n"
            "    assert config.accounts.bindings\n"
            "    assert not hasattr(config, 'author_accounts')\n"
        ),
    )
    with preloaded_workers():
        check_validation(parameters, definition)
        validate_strategy_config(definition, economic, economic=True)


@pytest.mark.parametrize("preloaded", [False, True])
@pytest.mark.parametrize("executable", [False, True])
@pytest.mark.parametrize("reject", [False, True])
def test_public_and_runtime_validation_share_the_sandbox_contract(
    recorded, preloaded, executable, reject
):
    from almanak.checks import check_validation
    from almanak.configuration.parameter_codec import hook_parameters

    parameters = hook_parameters(recorded.config)
    names = tuple(binding.name for binding in parameters.accounts.bindings)
    source = (
        "def validate_config(config):\n"
        "    assert type(config).__name__ == 'LendingParameters'\n"
        f"    assert tuple(b.name for b in config.accounts.bindings) == {names!r}\n"
        "    assert config.accounts.binding(config.allocations[0].account).profile.fund.chain "
        "== config.allocations[0].chain\n"
        "    assert not hasattr(config, 'production')\n"
        "    assert not hasattr(config, 'fork_bindings')\n"
        "    assert not hasattr(config, 'author_accounts')\n"
    )
    if reject:
        source += "    raise ValueError('rejected economic choice')\n"
    definition = StrategyDefinition(filename="strategy.py", source=source)
    config = recorded.config if executable else parameters
    with preloaded_workers() if preloaded else nullcontext():
        for invoke in (
            lambda: check_validation(config, definition),
            lambda: validate_strategy_config(definition, config),
        ):
            if reject:
                with pytest.raises(StrategyConfigRefused, match="rejected economic choice") as e:
                    invoke()
                assert (e.value.line, e.value.field) == (8, "strategy.parameters")
            else:
                invoke()
