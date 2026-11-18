import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak.cli import main
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.connectors.aave_v4.reader import plan_operation
from almanak.connectors.composition import connector_for
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.fork_fixture import Fork
from almanak.state import Journal
from tests.reference_parameters import authored_values

CONFIG = Path(__file__).parents[1] / "tests/fixtures/author/aave_v4_weth_supply/config.json"


def test_check_is_offline_and_reports_authored_inputs(monkeypatch, capsys, tmp_path):

    from tests.test_author_resolution import REFERENCE

    author = tmp_path / "config.yaml"
    author.write_bytes((REFERENCE / "config.yaml").read_bytes())
    (tmp_path / "strategy.py").write_bytes((REFERENCE / "strategy.py").read_bytes())
    monkeypatch.setattr(
        "sys.argv",
        ["almanak", "check", str(tmp_path), "--config", str(author), "--normalized", "--json"],
    )
    assert main() == 0
    result = json.loads(capsys.readouterr().out)
    authored = result["normalized"]["authored"]
    assert authored["strategy"]["allocations"]["main_weth"]["minimum_native_balance"] == "0.001"
    assert authored["strategy"]["allocations"]["main_weth"]["capital"] == "0.01"
    assert result["normalized"]["parameters"] is None
    assert result["valid"] and not result["ready"]
    assert {s["stage"]: s["status"] for s in result["stages"]}["resolution"] == "not_checked"


@pytest.mark.parametrize(
    "field,value",
    [
        ("minimum_native_balance", -1),
        ("minimum_native_balance", "10"),
        ("capital", -1),
        ("amount", 1),
        ("unknown_risk", 1),
    ],
)
def test_invalid_parameters_fail_before_execution(field, value):
    raw = json.loads(CONFIG.read_text())
    raw["allocations"][0][field] = value
    with pytest.raises(ValidationError):
        ForkConfig.model_validate_json(json.dumps(raw))


@pytest.mark.parametrize("kind", ["approve", "supply", "withdraw", "revoke"])
@pytest.mark.parametrize("below", [True, False])
def test_gateway_enforces_threshold_only_for_new_risk(recorded, kind, below):
    observed_kind = "supply" if kind == "revoke" else kind
    outcome = next(item for item in recorded.outcomes if item.operation.kind == observed_kind)
    before = outcome.before
    allocation = recorded.config.allocations[0].model_copy(
        update={
            "minimum_native_balance": before.native_balance.amount + int(below),
        }
    )
    config = recorded.config.model_copy(update={"allocations": (allocation,)})
    shares = before.position.supplied_shares
    state = recorded.model_copy(
        update={
            "config": config,
            "mode": "running" if kind in ("approve", "supply") else "teardown",
            "outcomes": (),
            "delivered": 0,
            "owned": (recorded.owned[0].model_copy(update={"shares": shares}),),
        }
    )
    backend = SimpleNamespace(
        config=config,
        connector=connector_for(config),
        reader=SimpleNamespace(chain=before.target.fund.chain),
        guard=lambda: None,
    )
    intent = (
        config.supply(allocation.name, allocation.capital)
        if kind in ("approve", "supply")
        else config.withdraw(allocation.name)
    )
    operation = plan_operation(intent, before)
    state = state.model_copy(
        update={"decision": outcome.decision.model_copy(update={"intent": intent})}
    )
    if below and kind in ("approve", "supply"):
        with pytest.raises(DataError, match="configured minimum"):
            ForkGateway.validate_operation(backend, state, operation, before)
    else:
        ForkGateway.validate_operation(backend, state, operation, before)


def test_fork_preparation_preserves_parameters(tmp_path, reference_strategy):
    config = ForkConfig.model_validate_json(
        (Path(__file__).parents[1] / "tests/fixtures/aave-v4-interval-config.json").read_text()
    )
    node = object.__new__(Fork)
    node.origin = config.origin
    node.metadata = {"instanceId": "0x" + "12" * 32}
    node.rpc = SimpleNamespace(endpoint="http://127.0.0.1:1234")
    node.funded_signer = lambda *args, **kwargs: SimpleNamespace(
        address="0x" + "34" * 20,
        key=bytes.fromhex("56" * 32),
    )
    deployment = node.prepare(config, tmp_path / "deployment")
    # The legacy fixture deployment predates strategy.parameters; the knobs are the reference's.
    deployment = deployment.model_copy(
        update={"parameters": authored_values("aave_v4_weth_supply")}
    )
    allocation = deployment.allocations[0]
    assert allocation.capital == config.allocations[0].capital
    assert allocation.minimum_native_balance == 10**15
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(deployment, reference_strategy)
    assert journal.load().config == deployment
