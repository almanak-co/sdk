"""Branch coverage for the replay CLI (engine + click command).

Covers ``ReplayEngine.replay`` step sequencing (anvil failure, action
success/failure, missing action bundle, verbose prints, outer error path)
and the ``replay`` click command's option handling — with Anvil, bundle
storage and the engine faked. No forks, no network.
"""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from almanak.framework.cli.replay import (
    ReplayContext,
    ReplayEngine,
    ReplayStepType,
    replay,
)
from almanak.framework.models.reproduction_bundle import ReproductionBundle


def _bundle(**overrides):
    defaults = {
        "deployment_id": "deployment:abc123",
        "failure_timestamp": datetime.now(UTC),
        "block_number": 123456,
        "chain": "arbitrum",
        "persistent_state": {"position": "open"},
        "config": {"slippage": 0.01},
    }
    defaults.update(overrides)
    return ReproductionBundle(**defaults)


def _action_bundle():
    return SimpleNamespace(
        intent_type="SWAP",
        to_dict=lambda: {"intent_type": "SWAP"},
    )


@pytest.fixture
def engine(monkeypatch) -> ReplayEngine:
    engine = ReplayEngine(verbose=False)
    engine.stopped = []
    monkeypatch.setattr(engine, "_start_anvil", lambda ctx: True)
    monkeypatch.setattr(engine, "_stop_anvil", lambda: engine.stopped.append(True))
    monkeypatch.setattr(engine, "_get_current_state", lambda ctx: {"balance": "1"})
    monkeypatch.setattr(engine, "_execute_action", lambda ab, ctx: {"status": "ok"})
    return engine


def _step_types(result):
    return [step.step_type for step in result.steps]


class TestReplayEngine:
    def test_anvil_failure_aborts(self, engine, monkeypatch):
        monkeypatch.setattr(engine, "_start_anvil", lambda ctx: False)
        result = engine.replay(ReplayContext(bundle=_bundle()))
        assert not result.success
        assert result.error == "Failed to start Anvil fork"
        assert _step_types(result) == [ReplayStepType.INITIALIZE]
        assert engine.stopped == [True]

    def test_replay_without_action_bundle(self, engine):
        result = engine.replay(ReplayContext(bundle=_bundle()))
        assert result.success
        assert _step_types(result) == [
            ReplayStepType.INITIALIZE,
            ReplayStepType.LOAD_STATE,
            ReplayStepType.LOAD_CONFIG,
            ReplayStepType.VERIFY_STATE,
            ReplayStepType.COMPLETE,
        ]
        assert result.final_state == {"balance": "1"}
        assert result.duration_seconds is not None

    def test_replay_with_action_bundle(self, engine):
        result = engine.replay(ReplayContext(bundle=_bundle(action_bundle=_action_bundle())))
        assert result.success
        assert ReplayStepType.EXECUTE_ACTION in _step_types(result)
        action_step = next(
            s for s in result.steps if s.step_type == ReplayStepType.EXECUTE_ACTION
        )
        assert action_step.details["execution_result"] == {"status": "ok"}
        assert action_step.state_after == {"balance": "1"}

    def test_action_failure_marks_result_failed(self, engine, monkeypatch):
        def _boom(action_bundle, ctx):
            raise RuntimeError("revert: slippage")

        monkeypatch.setattr(engine, "_execute_action", _boom)
        result = engine.replay(ReplayContext(bundle=_bundle(action_bundle=_action_bundle())))
        assert not result.success
        assert "revert: slippage" in result.error
        # Replay continues to verify/complete even after action failure.
        assert _step_types(result)[-1] == ReplayStepType.COMPLETE

    def test_verbose_paths_invoke_printers(self, engine, monkeypatch):
        printed = []
        monkeypatch.setattr(engine, "_print_state", lambda *a: printed.append("state"))
        monkeypatch.setattr(
            engine, "_print_market_data", lambda *a: printed.append("market")
        )
        monkeypatch.setattr(
            engine, "_print_events_before", lambda *a: printed.append("events")
        )
        monkeypatch.setattr(
            engine, "_print_action_result", lambda *a: printed.append("action")
        )
        market_data = SimpleNamespace(to_dict=lambda: {"price": "1"})
        bundle = _bundle(
            action_bundle=_action_bundle(),
            market_data=market_data,
            events_before=[SimpleNamespace()],
        )
        result = engine.replay(ReplayContext(bundle=bundle, verbose=True))
        assert result.success
        assert printed == ["state", "state", "market", "events", "action"]
        assert ReplayStepType.LOAD_MARKET_DATA in _step_types(result)

    def test_unexpected_error_records_error_step(self, engine):
        # A non-serializable persistent state breaks the LOAD_STATE step's
        # json.dumps and lands in the outer error handler.
        result = engine.replay(
            ReplayContext(bundle=_bundle(persistent_state={"bad": object()}))
        )
        assert not result.success
        assert _step_types(result)[-1] == ReplayStepType.ERROR
        assert engine.stopped == [True]


class _FakeResult:
    def __init__(self, success=True, error=None):
        self.success = success
        self.error = error

    def summary(self):
        return "fake summary"


class _FakeEngine:
    last = None

    def __init__(self, verbose=False):
        self.verbose = verbose
        type(self).last = self
        self.result = _FakeResult()

    def replay(self, ctx):
        self.ctx = ctx
        return self.result


@pytest.fixture
def cli(monkeypatch):
    monkeypatch.setattr("almanak.framework.cli.replay.ReplayEngine", _FakeEngine)
    _FakeEngine.last = None
    return CliRunner()


class TestReplayCommand:
    def test_list_bundles_flag(self, cli, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "almanak.framework.cli.replay.list_available_bundles",
            lambda: calls.append(True),
        )
        result = cli.invoke(replay, ["--list-bundles"])
        assert result.exit_code == 0
        assert calls == [True]

    def test_missing_bundle_arguments_aborts(self, cli):
        result = cli.invoke(replay, [])
        assert result.exit_code != 0
        assert "Must provide --bundle or --bundle-file" in result.output

    def test_bundle_file_not_found(self, cli, monkeypatch, tmp_path):
        # click validates the path exists; the FileNotFoundError branch guards
        # bundles that vanish (or nested paths) between validation and load.
        bundle_file = tmp_path / "x.json"
        bundle_file.write_text("{}")

        def _load(path):
            raise FileNotFoundError(f"no bundle at {path}")

        monkeypatch.setattr("almanak.framework.cli.replay.load_bundle_from_file", _load)
        result = cli.invoke(replay, ["--bundle-file", str(bundle_file)])
        assert result.exit_code != 0
        assert "--list-bundles" in result.output

    def test_bundle_file_invalid_json(self, cli, monkeypatch, tmp_path):
        import json as json_mod

        bundle_file = tmp_path / "x.json"
        bundle_file.write_text("{not json")

        def _load(path):
            raise json_mod.JSONDecodeError("bad", "{", 0)

        monkeypatch.setattr("almanak.framework.cli.replay.load_bundle_from_file", _load)
        result = cli.invoke(replay, ["--bundle-file", str(bundle_file)])
        assert result.exit_code != 0
        assert "Invalid JSON" in result.output

    def test_generic_load_error(self, cli, monkeypatch):
        def _fetch(bundle_id):
            raise RuntimeError("storage offline")

        monkeypatch.setattr("almanak.framework.cli.replay.fetch_bundle", _fetch)
        result = cli.invoke(replay, ["--bundle", "some-id"])
        assert result.exit_code != 0
        assert "Error loading bundle" in result.output

    def test_dry_run_prints_info_only(self, cli, monkeypatch):
        bundle = _bundle()
        printed = []
        monkeypatch.setattr(
            "almanak.framework.cli.replay.fetch_bundle", lambda bundle_id: bundle
        )
        monkeypatch.setattr(
            "almanak.framework.cli.replay.print_bundle_info",
            lambda b: printed.append(b),
        )
        result = cli.invoke(replay, ["--bundle", "some-id", "--dry-run"])
        assert result.exit_code == 0
        assert printed == [bundle]
        assert _FakeEngine.last is None

    def test_chain_and_block_overrides(self, cli, monkeypatch):
        bundle = _bundle()
        monkeypatch.setattr(
            "almanak.framework.cli.replay.fetch_bundle", lambda bundle_id: bundle
        )
        result = cli.invoke(
            replay,
            ["--bundle", "some-id", "--chain", "base", "--block", "999", "--verbose"],
        )
        assert result.exit_code == 0
        assert bundle.chain == "base"
        assert bundle.block_number == 999
        assert _FakeEngine.last.ctx.verbose is True
        assert "fake summary" in result.output

    def test_json_output_written(self, cli, monkeypatch, tmp_path):
        bundle = _bundle()
        written = []
        monkeypatch.setattr(
            "almanak.framework.cli.replay.fetch_bundle", lambda bundle_id: bundle
        )
        monkeypatch.setattr(
            "almanak.framework.cli.replay.write_json_output",
            lambda result, path: written.append(path),
        )
        out = tmp_path / "result.json"
        result = cli.invoke(replay, ["--bundle", "some-id", "--output", str(out)])
        assert result.exit_code == 0
        assert written == [out]

    def test_failed_replay_exits_nonzero(self, cli, monkeypatch):
        bundle = _bundle()
        monkeypatch.setattr(
            "almanak.framework.cli.replay.fetch_bundle", lambda bundle_id: bundle
        )

        class _FailingEngine(_FakeEngine):
            def __init__(self, verbose=False):
                super().__init__(verbose)
                self.result = _FakeResult(success=False, error="anvil crashed")

        monkeypatch.setattr("almanak.framework.cli.replay.ReplayEngine", _FailingEngine)
        result = cli.invoke(replay, ["--bundle", "some-id"])
        assert result.exit_code == 1
        assert "anvil crashed" in result.output
