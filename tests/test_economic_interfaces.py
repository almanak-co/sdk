"""Typed economic authoring and sandbox hooks do not grant live execution support."""

import io
import json
import socket
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter

from almanak.backtest.cli import create
from almanak.backtest.config import EconomicBacktestConfig
from almanak.backtest.economic_state import (
    EconomicEvent,
    EconomicEventInput,
    EconomicParameters,
    EconomicPosition,
    Intent,
)
from almanak.backtest.inputs import HarnessSelection
from almanak.configuration.parameter_codec import parameters_from_wire
from almanak.connectors.composition import connector_from_wire, installed_connectors
from almanak.effects import Effects
from almanak.lending import SupplyTarget
from almanak.lending_intents import (
    BorrowIntent,
    CollateralIntent,
    RepayAmountIntent,
    RepayIntent,
    WithdrawAmountIntent,
)
from almanak.models import AssetAmount
from almanak.runtime import strategy_worker
from almanak.strategy import EventRequest, StrategyDefinition
from almanak.strategy_api import Memory, SimulationEvidence
from almanak.strategy_worker import evaluate_request, read_request
from tests.backtest_support import ROOT


def economic_settings():
    raw = json.loads((ROOT / "tests/fixtures/author/aave_v4_weth_supply/config.json").read_text())
    raw["backtest"] = json.loads(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/backtest.json").read_text()
    )
    raw["backtest"].update(
        market_model="economic",
        data={
            "source": "aave_v4_archive",
            "rate_sample_seconds": 300,
            "risk_prices": "spoke_oracle_events",
        },
    )
    return raw


def public_parameters(raw):
    """Present the retained fork fixture through the current strategy hook contract."""
    return parameters_from_wire(
        {
            "connector": "aave_v4",
            "decision_trigger": raw["decision_trigger"],
            "allocations": raw["allocations"],
            "debts": raw["debts"],
            "asset_metadata": raw.get("asset_metadata", {}),
            "parameters": raw.get("parameters", {}),
            "author_accounts": {
                fund["account"]: {"chain": raw["forks"][0]["chain"]} for fund in raw["funds"]
            },
        }
    )


def test_economic_mode_never_silently_enters_the_detailed_driver(tmp_path, monkeypatch):
    raw = economic_settings()
    config = tmp_path / "config.json"
    backtest_config = tmp_path / "backtest.json"
    backtest_config.write_text(json.dumps(raw.pop("backtest")))
    config.write_text(json.dumps(raw))
    author = public_parameters(raw)
    assert (
        EconomicBacktestConfig.model_validate_json(
            backtest_config.read_text()
        ).data.rate_sample_seconds
        == 300
    )
    assert author.decision_trigger.seconds == 5

    def forbidden(*args, **kwargs):
        pytest.fail("economic refusal must precede gateway provisioning")

    monkeypatch.setattr("almanak.backtest.cli.provision", forbidden)
    settings = EconomicBacktestConfig.model_validate_json(backtest_config.read_text())
    strategy = StrategyDefinition.read(ROOT / "strategies/aave_v4_weth_supply/strategy.py")
    with pytest.raises(ValueError, match="missing|offline"):
        create(
            SimpleNamespace(
                config=config,
                backtest_config=backtest_config,
                output=tmp_path / "run",
                strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
                _backtest_inputs=HarnessSelection(author, settings, strategy),
            )
        )


def test_economic_lending_variants_preserve_identity_and_live_support(recorded):
    target = recorded.config.target(recorded.config.allocations[0].name)
    amount = AssetAmount(asset=target.asset, amount=3)
    intents = (
        BorrowIntent[SupplyTarget](target=target, amount=amount),
        RepayAmountIntent[SupplyTarget](target=target, amount=amount),
        RepayIntent[SupplyTarget](target=target),
        WithdrawAmountIntent[SupplyTarget](target=target, amount=amount),
        CollateralIntent[SupplyTarget](target=target, enabled=True),
    )
    economic = TypeAdapter(Intent)
    from almanak.lending import Intent as LiveIntent

    live = TypeAdapter(LiveIntent)
    for intent in intents:
        assert economic.validate_json(intent.model_dump_json()) == intent
        if isinstance(intent, (BorrowIntent, RepayIntent)):
            assert live.validate_json(intent.model_dump_json()) == intent
        else:
            with pytest.raises(ValueError):
                live.validate_json(intent.model_dump_json())
    for cls in (
        BorrowIntent[SupplyTarget],
        RepayAmountIntent[SupplyTarget],
        WithdrawAmountIntent[SupplyTarget],
    ):
        with pytest.raises(ValueError, match="positive"):
            cls(target=target, amount=AssetAmount(asset=target.asset, amount=0))
        with pytest.raises(ValueError, match="asset"):
            cls(
                target=target,
                amount=AssetAmount(
                    asset=recorded.config.allocations[0].native_asset,
                    amount=3,
                ),
            )


def event_request(recorded, source):
    config = EconomicParameters(
        decision_trigger=recorded.config.decision_trigger,
        accounts=recorded.config.accounts,
        allocations=recorded.config.allocations,
    )
    return EventRequest(
        strategy=StrategyDefinition(filename="strategy.py", source=source),
        context=EconomicEventInput(
            config=config,
            memory=Memory(values={"events": 2}),
            event=EconomicEvent(
                effective_at=datetime(2026, 4, 1, tzinfo=UTC),
                liquidations=(),
                deficits=(),
                simulation=SimulationEvidence(execution_id="event:0", model="fixture", inputs=()),
            ),
            effects=Effects[EconomicPosition](),
        ),
    )


def test_optional_event_hook_returns_only_memory(monkeypatch, capsys, recorded):
    request = event_request(recorded, "def on_event(context): return context.memory")
    monkeypatch.setattr("sys.argv", ["worker", "--event"])
    monkeypatch.setattr("sys.stdin", io.StringIO(request.model_dump_json()))
    evaluate_request(*read_request())
    assert Memory.model_validate_json(capsys.readouterr().out).values == {"events": 2}


@pytest.mark.parametrize(
    "source,expected",
    [
        ("pass", {"events": 2}),
        (
            "from almanak.strategy_api import Memory\n"
            "def on_event(context):\n"
            "    assert context.event.effective_at.year == 2026\n"
            "    return Memory(values={'events': context.memory.values['events'] + 1})",
            {"events": 3},
        ),
    ],
)
def test_actual_sandbox_event_handler_preserves_optional_hook_semantics(recorded, source, expected):
    result = strategy_worker(event_request(recorded, source))
    assert Memory.model_validate_json(result).values == expected


def test_event_handler_cannot_return_an_intent_or_modify_schedule(recorded):
    source = (
        "from almanak.lending import Decision\n"
        "def on_event(context):\n"
        "    return Decision(memory=context.memory)"
    )
    with pytest.raises(RuntimeError, match="strategy worker failed"):
        strategy_worker(event_request(recorded, source))
    source = "def on_event(context):\n    context.config.decision_trigger.seconds = 1"
    with pytest.raises(RuntimeError, match="frozen"):
        strategy_worker(event_request(recorded, source))


def test_event_worker_cannot_read_the_dataset_or_contact_host_network(recorded, tmp_path):
    path = tmp_path / "prepared.sqlite"
    path.write_text("future input must stay private")
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        with socket.create_connection(server.getsockname(), timeout=2):
            pass
        source = f"""
from pathlib import Path
import socket
from almanak.strategy_api import Memory

def on_event(context):
    denied = 0
    try:
        Path({str(path)!r}).read_text()
    except OSError:
        denied += 1
    try:
        socket.create_connection({server.getsockname()!r}, timeout=1)
    except OSError:
        denied += 1
    return Memory(values={{'denied': denied}})
"""
        assert Memory.model_validate_json(
            strategy_worker(event_request(recorded, source))
        ).values == {
            "denied": 2,
        }


def test_every_declared_backtest_binding_resolves():
    connectors = (*(c for _, c in installed_connectors()), connector_from_wire({"connector": None}))
    for connector in connectors:
        for mode, declared in (
            ("historical_path", connector.historical),
            ("economic", connector.economic),
        ):
            if declared is not None:
                assert type(connector.backtest(mode)).__name__ == declared.split(":")[1]
