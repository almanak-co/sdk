"""Current Curve compositions replay captured inputs without provider access."""

import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.cli import GatewayProcess
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.strategy import StrategyDefinition
from tests.composed_captures import rebind
from tests.composed_public import economic_inputs
from tests.lending_captures import current_record

ROOT = Path(__file__).parents[1]
CAPTURE = ROOT / "docs/internal/reports/evidence/2026-09-09-rollout/curve-composition-prepared"


def captured(path):
    files = json.loads((CAPTURE / "manifest.json").read_text())["files"]
    expected = next(item["sha256"] for item in files if item["path"] == path)
    value = (CAPTURE / path).read_bytes()
    assert hashlib.sha256(value).hexdigest() == expected
    return gzip.decompress(value)


@pytest.fixture(params=["base", "arbitrum"])
def prepared(request, tmp_path, monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("captured composition replay cannot access providers")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    monkeypatch.setattr(GatewayProcess, "__enter__", forbid)
    reference = ROOT / "strategies" / f"{request.param}_aave_curve_borrow_and_trade"
    author, config, settings, interval = economic_inputs(reference.name)
    dataset = tmp_path / "dataset"
    names = [
        "dataset.json",
        *(
            f"{connector}/{name}"
            for connector in ("aave_v3", "curve")
            for name in ("dataset.json", "data.sqlite")
        ),
    ]
    for name in names:
        target = dataset / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(captured(f"isolated/{request.param}/dataset/{name}.gz"))
    recorded = json.loads(captured(f"isolated/{request.param}/run/manifest.json.gz"))
    current = current_record(recorded)
    # The retained trade allocation names a `hold_seconds` that PR 175 removed from this
    # reference as dead: its strategy never reads it and the SDK schedules no hold.
    assert "hold_seconds" not in (reference / "strategy.py").read_text()
    for allocation in current["config"]["allocations"]:
        allocation.pop("hold_seconds", None)
    rebind(dataset, recorded, config, settings, current)
    strategy = StrategyDefinition.read(reference / "strategy.py")
    args = SimpleNamespace(
        config=reference / "config.yaml",
        strategy=reference / "strategy.py",
        backtest_config=reference / "backtest-economic.yaml",
        dataset=dataset,
        output=tmp_path / "run",
        _backtest_inputs=(author, settings, strategy),
    )
    return SimpleNamespace(
        args=args,
        author=author,
        settings=settings,
        config=config,
        strategy=strategy,
        interval=interval,
        dataset=dataset,
    )


def test_current_reference_runs_resumes_and_reproduces_offline(prepared):
    from almanak.backtest.economic_cli import create, reproduce, resume
    from almanak.effects import WalletAsset

    report = create(prepared.args, prepared.author, prepared.settings)
    assert report["closed"] and report["completed_intents"] == 6
    assert report["delivered_outcomes"] == 6
    assert report["provider_requests_during_execution"] == 0
    initial = ComposedEconomic.initial(prepared.settings, prepared.config, prepared.interval)
    assert {
        WalletAsset.model_validate_json(json.dumps(wallet["key"])) for wallet in report["wallets"]
    } == {balance.key for balance in initial.portfolio.balances}
    args = SimpleNamespace(run=prepared.args.output)
    assert resume(args)["closed"]
    assert reproduce(args)["reproduced"]


def test_curve_descriptor_refuses_native_manifest_substitution(prepared):
    from almanak.connectors.composed.economic_book import Book

    book = Book(prepared.dataset, prepared.config, prepared.settings)
    book.close()
    path = prepared.dataset / "curve/dataset.json"
    path.write_text(path.read_text() + "\n")
    with pytest.raises(ValueError, match="manifest differs from the frozen run"):
        Book(prepared.dataset, prepared.config, prepared.settings)
