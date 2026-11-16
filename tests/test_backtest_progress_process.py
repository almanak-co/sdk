"""Terminal, callback and checkpoint integration using the actual reference strategy."""

import json
from types import SimpleNamespace

import pytest

from almanak.backtest import cli, economic_prepare, progress
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.inputs import HarnessSelection
from almanak.backtest.prepared import DatasetWriter
from almanak.backtest.progress import ProgressJob
from almanak.backtest.progress_types import ProgressSnapshot
from almanak.configuration.parameter_codec import parameters_from_wire
from almanak.strategy import StrategyDefinition
from tests.backtest_support import ROOT
from tests.fork.eoa import configuration
from tests.test_backtest_process import archive as archive
from tests.test_backtest_process import run_cli
from tests.test_economic_cli import synthetic_dataset
from tests.test_economic_math import reserve as reserve
from tests.test_economic_model import simulation as simulation
from tests.test_economic_prepare import acquisition as acquisition


def snapshots(text):
    return [ProgressSnapshot.model_validate_json(row) for row in text.splitlines()]


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


def test_historical_terminal_json_and_seeded_start(archive, tmp_path):
    _, config, operator, env_file = archive
    common = [
        "backtest",
        "run",
        "--strategy",
        str(ROOT / "strategies/aave_v4_weth_supply/strategy.py"),
        "--config",
        str(config),
        "--backtest-config",
        str(config.with_name("backtest.yaml")),
        "--gateway-config",
        str(operator),
        "--env-file",
        str(env_file),
        "--progress",
        "json",
    ]
    first = run_cli([*common, "--output", str(tmp_path / "first")])
    assert first.returncode == 0, first.stderr
    first_rows = snapshots(first.stderr)
    assert json.loads(first.stdout)["closed"]
    assert first_rows[-1].status == "completed"
    assert {r.phase for r in first_rows} >= {"startup", "simulation", "teardown", "finalization"}
    assert all(p.finished for p in first_rows[-1].phases)
    second = run_cli([*common, "--output", str(tmp_path / "second")])
    assert second.returncode == 0, second.stderr
    assert snapshots(second.stderr)[0].eta.basis == "previous_runs"
    assert snapshots(second.stderr)[0].eta.remaining_seconds > 0
    path = tmp_path / "first.progress.json"
    assert ProgressSnapshot.model_validate_json(path.read_text()) == first_rows[-1]
    resumed = run_cli(
        ["backtest", "resume", "--run", str(tmp_path / "first"), "--progress", "json"]
    )
    assert resumed.returncode == 0, resumed.stderr
    resumed_rows = snapshots(resumed.stderr)
    assert resumed_rows[-1].job_id == first_rows[-1].job_id
    assert resumed_rows[0].sequence > first_rows[-1].sequence
    reproduced = run_cli(["backtest", "reproduce", "--run", str(tmp_path / "first")])
    assert reproduced.returncode == 0, reproduced.stderr
    assert ProgressSnapshot.model_validate_json(path.read_text()) == resumed_rows[-1]
    assert (tmp_path / "first.reproduce.progress.json").is_file()


def test_acquisition_progress_restores_committed_ranges(acquisition, tmp_path, monkeypatch):
    args, _, _ = acquisition
    path = tmp_path / "data.progress.json"
    original = DatasetWriter.commit_range
    interrupted = []

    def commit(writer, span, records, evidence):
        original(writer, span, records, evidence)
        if span.key.product == "interest" and not interrupted:
            interrupted.append(span.key)
            raise RuntimeError("interrupt after durable range")

    monkeypatch.setattr(DatasetWriter, "commit_range", commit)
    with pytest.raises(RuntimeError, match="durable range"):
        with ProgressJob(path, "capture", interval=3600):
            economic_prepare.prepare(args)
    before = ProgressSnapshot.model_validate_json(path.read_text())
    assert before.status == "failed"
    assert before.eta.remaining_seconds is None
    seen = []
    with ProgressJob(path, "capture", callback=seen.append, resume=True, interval=3600):
        result = economic_prepare.prepare(args)
    assert result["dataset"]["records"] == 26
    assert result["provider_activity"]["attempts"] == 0
    accounting = [r for r in seen if r.phase and "accounting_samples" in r.phase]
    restored = next(p for p in accounting[0].phases if p.phase_id == accounting[0].phase)
    assert restored.completed_units == restored.total_units > 0
    assert all(p.finished for p in seen[-1].phases)
    assert seen[-1].job_id == before.job_id
    with ProgressJob(path, "reused", callback=seen.append, resume=True, interval=3600):
        economic_prepare.prepare(args)
    assert [p.phase_id for p in seen[-1].phases] == ["preparation.reuse"]


def test_native_aave_acquisition_inside_composition_keeps_sibling_progress(acquisition, tmp_path):
    args, _, _ = acquisition
    with ProgressJob(tmp_path / "composed.progress.json", "composed", interval=3600) as job:
        with progress.phase_scope("curve"):
            with progress.phase("preparation", "Retained sibling", 3):
                progress.advance(3)
        sibling = job.phases["curve.preparation"]
        with progress.phase_scope("aave_v4"):
            result = economic_prepare.prepare(args)
        assert result["dataset"]["records"] == 26
        assert result["provider_activity"]["attempts"] == 0
        assert all(name.startswith(("curve.", "aave_v4.")) for name in job.phases)
        assert all(phase.finished for phase in job.phases.values())
        with progress.phase_scope("aave_v4"):
            assert economic_prepare.prepare(args) == result["dataset"]
        assert set(job.phases) == {"curve.preparation", "aave_v4.preparation.reuse"}
        assert job.phases["curve.preparation"] is sibling
        assert sibling.completed == 3 and sibling.finished


def test_economic_command_and_observer_failure_preserve_offline_state(
    simulation, tmp_path, monkeypatch, capsys
):
    journal, model = simulation
    dataset = tmp_path / "dataset"
    synthetic_dataset(dataset, model)
    config, backtest = tmp_path / "config.json", tmp_path / "backtest.json"
    raw = configuration()
    config.write_text(json.dumps(raw))
    backtest.write_text(model.settings.model_dump_json())
    strategy = StrategyDefinition.read(ROOT / "strategies/aave_v4_weth_supply/strategy.py")
    args = SimpleNamespace(
        product="backtest",
        strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
        config=config,
        backtest_config=backtest,
        dataset=dataset,
        output=tmp_path / "run",
        env_file=None,
        progress="json",
        _backtest_inputs=HarnessSelection(public_parameters(raw), model.settings, strategy),
    )
    delivered = []
    report = cli.run_with_progress(args, on_progress=delivered.append)
    captured = capsys.readouterr()
    assert report["provider_requests_during_execution"] == 0
    assert captured.out == ""
    rows = snapshots(captured.err)
    assert rows == delivered
    assert all(not p.phase_id.startswith("preparation") for p in rows[-1].phases)
    assert rows[-1].decision_evaluations == 8
    initial = journal.load()
    expected = Simulator(journal, model).run()
    replay = Journal(tmp_path / "observed.sqlite", journal.state_type)
    replay.create(initial)

    def broken(snapshot):
        raise RuntimeError("progress callback failed")

    with ProgressJob(tmp_path / "observed.progress.json", "test", callback=broken, interval=3600):
        actual = Simulator(replay, model).run()
    assert actual == expected
    assert progress.current() is None


def test_auto_prepare_then_cached_execution_reports_one_job(
    simulation, tmp_path, monkeypatch, capsys
):
    _, model = simulation
    config, backtest = tmp_path / "config.json", tmp_path / "backtest.json"
    raw = configuration()
    config.write_text(json.dumps(raw))
    backtest.write_text(model.settings.model_dump_json())
    strategy = StrategyDefinition.read(ROOT / "strategies/aave_v4_weth_supply/strategy.py")

    def prepare(args):
        if not (args.output / "dataset.json").exists():
            with progress.phase("preparation", "Synthetic fixture preparation", 1.0, "phases"):
                synthetic_dataset(args.output, model)
        else:
            with progress.phase("preparation.reuse", "Verifying cached dataset", 1.0, "phases"):
                pass

    monkeypatch.setattr(economic_prepare, "prepare", prepare)
    args = SimpleNamespace(
        product="backtest",
        strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
        config=config,
        backtest_config=backtest,
        dataset=None,
        output=tmp_path / "first",
        env_file=None,
        progress="json",
        _backtest_inputs=HarnessSelection(public_parameters(raw), model.settings, strategy),
    )
    cli.run_with_progress(args)
    first = snapshots(capsys.readouterr().err)
    assert any(r.phase == "preparation" for r in first)
    assert len({r.job_id for r in first}) == 1
    args.output = tmp_path / "second"
    cli.run_with_progress(args)
    second = snapshots(capsys.readouterr().err)
    assert any(r.phase == "preparation.reuse" for r in second)
    assert first[-1].timing_key != second[-1].timing_key
    assert all(p.finished for p in second[-1].phases)
