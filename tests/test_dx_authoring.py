from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.checks import CheckFailure, check
from almanak.configuration.backtest_inputs import read_backtest_inputs
from almanak.configuration.metadata import verify
from almanak.gateway.ethereum import DataError
from almanak.memory_codec import freeze, hydrate
from almanak.models import Asset, Chain
from almanak.strategy_api import Memory, StrategyState
from tests.qa.catalog import CANDIDATES

ROOT = Path(__file__).parents[1]
REFERENCES = sorted((ROOT / "strategies").glob("*/config.yaml"))


def test_backtest_inputs_are_frozen_for_one_invocation(tmp_path):
    from almanak.backtest.inputs import selected, strategy_source
    from tests.test_author_resolution import REFERENCE

    for name in ("config.yaml", "backtest.yaml"):
        (tmp_path / name).write_bytes((REFERENCE / name).read_bytes())
    (tmp_path / "strategy.py").write_bytes((REFERENCE / "strategy.py").read_bytes())
    operator = tmp_path / "operator.yaml"
    operator.write_bytes((ROOT / "examples/operators/backtest/eip155-1.yaml").read_bytes())
    args = SimpleNamespace(
        config=tmp_path / "config.yaml",
        backtest_config=tmp_path / "backtest.yaml",
        strategy=tmp_path / "strategy.py",
        gateway_config=operator,
    )
    original = selected(args)
    for path in (args.config, args.backtest_config, args.strategy, operator):
        path.write_text("invalid changed input")
    assert selected(args) is original
    assert strategy_source(args) is original.strategy
    with pytest.raises(ValueError):
        selected(SimpleNamespace(**{k: v for k, v in vars(args).items() if not k.startswith("_")}))


def test_author_capability_stage_does_not_repeat_strategy_validation(capsys):
    from almanak.configuration.parameter_codec import validation_parameters
    from almanak.strategy import StrategyDefinition
    from almanak.strategy_worker import validate_author
    from tests.test_author_hook_capture import placeholder_accounts
    from tests.test_author_resolution import frozen_parameters

    author = frozen_parameters(placeholder_accounts())
    source = (
        "from almanak.author_capabilities import declared\n"
        "def validate_config(config):\n"
        "    raise AssertionError('validation ran twice')\n"
        "def capabilities(config):\n"
        "    return declared(config)\n"
    )
    wire = {
        "strategy": StrategyDefinition(filename="strategy.py", source=source).model_dump(),
        "config": validation_parameters(author).model_dump(mode="json"),
    }
    validate_author(wire, capabilities=True)
    assert capsys.readouterr().out.strip() == "null"


def test_online_check_requires_explicit_operator_inputs_and_mode():
    from almanak.checks import require_online_inputs
    from almanak.connectors.support import RunMode

    with pytest.raises(ValueError, match="requires --gateway-config and --env-file"):
        require_online_inputs(SimpleNamespace(gateway_config=Path("op.yaml")), RunMode.FORK)
    with pytest.raises(ValueError, match="requires --fork, --production or --backtest-config"):
        require_online_inputs(
            SimpleNamespace(gateway_config=Path("op.yaml"), env_file=Path("x.env")), None
        )


@pytest.mark.parametrize("path", REFERENCES, ids=lambda path: path.parent.name)
def test_every_author_passes_the_same_sandbox_check(path):
    from almanak.configuration.yaml import ConfigurationError

    # Aave V3 and candidate GMX V2 both declare these chains, so the author check accepts
    # the composition; its composed lifecycle proof is a blocked catalog case, not a refusal.
    checked_compositions = {
        "arbitrum_aave_borrow_and_trade_gmx",
        "avalanche_aave_borrow_and_trade_gmx",
    }
    args = SimpleNamespace(directory=path.parent, config=None, strategy=None)
    if path.parent.name in CANDIDATES and path.parent.name not in checked_compositions:
        with pytest.raises(ConfigurationError, match="connector does not support eip155"):
            check(args)
        return
    result = check(args)
    assert result["valid"] and not result["ready"]
    assert {s["stage"] for s in result["stages"] if s["status"] == "passed"} == {
        "configuration",
        "sandbox",
    }


@pytest.mark.parametrize("path", REFERENCES, ids=lambda path: path.parent.name)
def test_every_backtest_yaml_retains_explicit_sources_and_inventory(path):
    for settings_path in path.parent.glob("backtest*.yaml"):
        inputs = read_backtest_inputs(settings_path).inputs
        assert "history" not in inputs.data
        if inputs.mode == "economic":
            assert inputs.data
        assert inputs.inventory.funds
        assert all(fund.balances for fund in inputs.inventory.funds.values())
        assert inputs.window.start < inputs.window.end
        assert inputs.costs


def test_precision_mismatch_fails_at_trusted_boundary():
    from tests.test_author_resolution import frozen_fork

    author = frozen_fork()
    asset = author.allocations[0].reserve.asset
    metadata = Asset(
        chain=Chain(chain_id=asset.chain, environment="mainnet"), identifier=asset, decimals=6
    )
    with pytest.raises(DataError, match="configured decimals"):
        verify(author, metadata)


def test_optional_typed_memory_freezes_only_json_and_rejects_schema_changes():
    class State(StrategyState):
        count: int = 0

    namespace = {"State": State}
    initial = hydrate(namespace, Memory())
    frozen = freeze(namespace, initial.updated(count=2))
    assert frozen.values == {"count": 2} and frozen.schema_id
    assert hydrate(namespace, Memory.model_validate_json(frozen.model_dump_json())).count == 2
    with pytest.raises(ValueError, match="schema"):
        hydrate(namespace, frozen.model_copy(update={"schema_id": "different"}))
    with pytest.raises(ValueError, match="untyped"):
        hydrate(namespace, Memory(values={"count": 1}))
    with pytest.raises(TypeError, match="declared State"):
        freeze(namespace, Memory())
    with pytest.raises(ValueError):
        initial.updated(count="invalid")


def test_updated_keeps_fraction_and_decimal_fields():
    class State(StrategyState):
        price: Fraction | None = None
        amount: Decimal | None = None
        exiting: bool = False

    namespace = {"State": State}
    initial = hydrate(namespace, Memory()).updated(price=Fraction(3, 7), amount=Decimal("1.5"))
    exiting = initial.updated(exiting=True)
    assert (exiting.price, exiting.amount, exiting.exiting) == (
        Fraction(3, 7),
        Decimal("1.5"),
        True,
    )
    frozen = freeze(namespace, exiting)
    restored = hydrate(namespace, Memory.model_validate_json(frozen.model_dump_json()))
    assert restored == exiting
    assert restored.updated(exiting=False).price == Fraction(3, 7)
    with pytest.raises(ValueError):
        exiting.updated(price="3/7")
    with pytest.raises(ValueError):
        exiting.updated(unknown=1)
    with pytest.raises(ValueError):
        exiting.updated(version=0)


def test_input_failures_precede_workspace_creation(tmp_path, monkeypatch):

    from almanak import workspace
    from tests.test_author_resolution import LEGACY, REFERENCE

    reference = tmp_path / "reference"
    reference.mkdir()
    (reference / "strategy.py").write_bytes((REFERENCE / "strategy.py").read_bytes())
    (reference / "config.yaml").write_bytes((LEGACY / "config.yaml").read_bytes())
    args = SimpleNamespace(
        directory=reference,
        config=None,
        strategy=None,
        fork=True,
        production=False,
        gateway_config=tmp_path / "operator.yaml",
        env_file=tmp_path / "operator.env",
        output=tmp_path / "run",
    )
    monkeypatch.setattr(workspace, "launch", lambda *_, **__: pytest.fail("launched"))
    with pytest.raises(CheckFailure) as failure:
        workspace.create(args)
    assert failure.value.diagnostic()["stages"][0]["status"] == "failed"
    assert not args.output.exists()
    (reference / "config.yaml").write_bytes((REFERENCE / "config.yaml").read_bytes())
    monkeypatch.setattr(
        "almanak.configuration.preparation.worker_command",
        lambda *_, **__: (_ for _ in ()).throw(RuntimeError("no sandbox")),
    )
    with pytest.raises(CheckFailure, match="no sandbox") as failure:
        workspace.create(args)
    stages = {s["stage"]: s["status"] for s in failure.value.diagnostic()["stages"]}
    assert stages["configuration"] == "passed" and stages["sandbox"] == "failed"
    assert stages["strategy"] == stages["online_admission"] == "not_checked"
    assert not args.output.exists()
