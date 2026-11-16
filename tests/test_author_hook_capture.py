"""Hook baseline collection preserves the sandbox and input-consistency boundary."""

import hashlib
import json
from pathlib import Path

import pytest

from scripts import capture_author_hooks as collector
from scripts.capture_config_baseline import canonical, digest
from tests.reference_parameters import authored_parameters
from tests.test_author_resolution import LEGACY, ROOT
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup

REFERENCE = Path(__file__).parents[1] / "strategies/aave_v4_weth_supply"
KNOBS = authored_parameters("aave_v4_weth_supply")


def placeholder_accounts():
    from almanak.models import StrategyAccounts

    bindings = [
        {
            "name": f"lending_{index}",
            "profile": {
                "kind": "eoa",
                "fund": {
                    "chain": {"chain_id": "eip155:1", "environment": "mainnet"},
                    "address": "0x" + pair * 20,
                },
            },
        }
        for index, pair in enumerate(("12", "34"))
    ]
    return StrategyAccounts.model_validate_json(json.dumps({"bindings": bindings}))


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    # Component tests control Git provenance; retained captures use the real clean-source guard.
    monkeypatch.setattr(collector, "source_revision", lambda _: "component-test")
    paths = tuple(tmp_path / name for name in ("config.yaml", "strategy.py", "accounts.json"))
    # The frozen harness input predates strategy.parameters; the knobs are the reference's.
    legacy = (LEGACY / "config.yaml").read_text()
    knobs = "".join(f"    {key}: '{value}'\n" for key, value in KNOBS.authored().items())
    interval = "  decision_interval: 5s\n"
    paths[0].write_text(legacy.replace(interval, interval + "  parameters:\n" + knobs, 1))
    paths[1].write_bytes((REFERENCE / "strategy.py").read_bytes())
    paths[2].write_text(placeholder_accounts().model_dump_json())
    return paths


def test_capture_runs_actual_reference_hooks_without_provider_access(inputs, monkeypatch):
    import httpx

    def forbidden(*_, **__):
        pytest.fail("hook capture contacted a provider")

    monkeypatch.setattr(httpx.Client, "post", forbidden)
    result = collector.capture(*inputs)
    assert len(result["capabilities"]["requirements"]) == 6
    assert result["dependencies"] == {"assets": [], "markets": [], "prices": []}
    assert result["validation_parameters"]["execution"] == {}
    assert "ALCHEMY_API_KEY" not in json.dumps(result)
    assert result["strategy_sha256"] == hashlib.sha256(inputs[1].read_bytes()).hexdigest()
    expected = {k: value for k, value in result.items() if k != "content_sha256"}
    assert result["content_sha256"] == digest(canonical(expected))


def test_capture_refuses_inputs_changed_during_hook_evaluation(inputs, monkeypatch):
    original = collector.validate_strategy_config

    def changed(definition, author):
        original(definition, author)
        inputs[0].write_text("changed after validation")

    monkeypatch.setattr(collector, "validate_strategy_config", changed)
    with pytest.raises(ValueError, match="input changed"):
        collector.capture(*inputs)


def test_capture_rejects_transport_environment_before_hooks(inputs, monkeypatch):
    raw = json.loads(inputs[2].read_text())
    raw["bindings"][0]["profile"]["fund"]["chain"]["environment"] = "anvil:test"
    inputs[2].write_text(json.dumps(raw))
    monkeypatch.setattr(collector, "validate_strategy_config", lambda *_: pytest.fail("hook ran"))
    with pytest.raises(ValueError, match="origin account bindings"):
        collector.capture(*inputs)


def test_public_root_hooks_are_captured_over_a_retained_resolution(tmp_path, monkeypatch, setup):
    import yaml

    from almanak.configuration.yaml import load
    from tests.test_author_resolution import parsed, resolve

    monkeypatch.setattr(collector, "source_revision", lambda _: "component-test")
    raw = load(REFERENCE / "config.yaml").value
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump(raw, sort_keys=False))
    strategy = tmp_path / "strategy.py"
    strategy.write_bytes((REFERENCE / "strategy.py").read_bytes())
    accounts = tmp_path / "accounts.json"
    accounts.write_text(placeholder_accounts().model_dump_json())
    with pytest.raises(ValueError, match="--resolution"):
        collector.capture(config, strategy, accounts)
    progress = resolve(parsed(raw), setup)
    resolution = tmp_path / "resolution.json"
    resolution.write_text(progress.model_dump_json())
    before = len(setup.rpc.calls)
    result = collector.capture_resolved(config, strategy, resolution)
    assert len(setup.rpc.calls) == before
    assert len(result["capabilities"]["requirements"]) == 6
    assert result["dependencies"] == {"assets": [], "markets": [], "prices": []}
    assert result["bound_parameters"]["allocations"][0]["capital"] == 10**16
    assert result["validation_parameters"] == result["bound_parameters"]
    assert "accounts" in result["validation_parameters"]
    assert "author_accounts" not in result["validation_parameters"]
    assert "execution" not in result["validation_parameters"]
    assert result["resolution_sha256"] == hashlib.sha256(resolution.read_bytes()).hexdigest()
    assert "hidden-input-secret" not in json.dumps(result)
    assert "ALCHEMY_API_KEY" not in json.dumps(result)


def test_capture_output_must_be_inside_the_evidence_directory(tmp_path):
    # A literal evidence path here would make the evidence guard skip this whole
    # module in hosted CI; the check needs the path, not the retained tree.
    for output in (collector.EVIDENCE, tmp_path / "capture.json"):
        with pytest.raises(ValueError, match="must remain under"):
            collector.main(
                [
                    "--config",
                    str(tmp_path / "config.yaml"),
                    "--strategy",
                    str(tmp_path / "strategy.py"),
                    "--accounts",
                    str(tmp_path / "accounts.json"),
                    "--output",
                    str(output),
                ]
            )


def test_public_composed_roots_require_resolution_before_hook_capture(tmp_path, monkeypatch):
    import httpx

    from almanak.models import StrategyAccounts

    monkeypatch.setattr(collector, "source_revision", lambda _: "component-test")
    source = ROOT / "strategies/base_borrow_and_trade"
    config, strategy = tmp_path / "config.yaml", tmp_path / "strategy.py"
    config.write_bytes((source / "config.yaml").read_bytes())
    strategy.write_bytes((source / "strategy.py").read_bytes())
    accounts = tmp_path / "accounts.json"
    binding = {
        "name": "fund",
        "profile": {
            "kind": "eoa",
            "fund": {
                "chain": {"chain_id": "eip155:8453", "environment": "mainnet"},
                "address": "0x" + "12" * 20,
            },
        },
    }
    bindings = StrategyAccounts.model_validate_json(json.dumps({"bindings": [binding]}))
    accounts.write_text(bindings.model_dump_json())
    monkeypatch.setattr(httpx.Client, "post", lambda *_, **__: pytest.fail("provider contacted"))
    with pytest.raises(ValueError, match="--resolution"):
        collector.capture(config, strategy, accounts)

    from almanak.configuration.preparation import check_preparation
    from almanak.configuration.unresolved import read_inputs
    from almanak.strategy import StrategyDefinition

    report = check_preparation(
        read_inputs(config), StrategyDefinition.read(strategy), normalized=True
    )
    stages = {item["stage"]: item["status"] for item in report["stages"]}
    authored = report["normalized"]["authored"]
    assert report["valid"] and not report["ready"]
    assert stages["sandbox"] == "passed"
    assert stages["strategy"] == stages["capabilities"] == "not_checked"
    assert authored["inventory"]
    assert set(authored["strategy"]["allocations"]) == {"collateral", "loan", "trade"}


def test_composed_collector_runs_actual_hooks_over_frozen_resolved_facts(tmp_path, monkeypatch):
    """The unresolved negative above does not replace composed hook coverage."""
    import httpx

    from almanak.models import StrategyAccounts

    monkeypatch.setattr(collector, "source_revision", lambda _: "component-test")
    source = ROOT / "strategies/base_borrow_and_trade"
    config = ROOT / "tests/fixtures/author/candidates/base_borrow_and_trade/config.yaml"
    strategy = tmp_path / "strategy.py"
    strategy.write_bytes((source / "strategy.py").read_bytes())
    accounts = tmp_path / "accounts.json"
    binding = {
        "name": "fund",
        "profile": {
            "kind": "eoa",
            "fund": {
                "chain": {"chain_id": "eip155:8453", "environment": "mainnet"},
                "address": "0x" + "12" * 20,
            },
        },
    }
    accounts.write_text(
        StrategyAccounts.model_validate_json(json.dumps({"bindings": [binding]})).model_dump_json()
    )
    monkeypatch.setattr(httpx.Client, "post", lambda *_, **__: pytest.fail("provider contacted"))
    result = collector.capture(config, strategy, accounts)
    assert result["dependencies"] == {"assets": [], "markets": [], "prices": []}
    assert {item["name"] for item in result["bound_parameters"]["allocations"]} == {
        "collateral",
        "loan",
        "trade",
    }
    assert len(result["capabilities"]["requirements"]) == 3
