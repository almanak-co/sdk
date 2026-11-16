"""Operational ETA evidence must not participate in simulated financial state."""

import io
import json
import threading
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from almanak.backtest import progress
from almanak.backtest.progress import Phase, ProgressJob, estimate, phase, total_eta
from almanak.backtest.progress_history import History
from almanak.backtest.progress_types import Eta, ProgressSnapshot


@pytest.fixture
def clock(monkeypatch):
    value = [100.0]
    monkeypatch.setattr(progress, "monotonic", lambda: value[0])
    return value


def test_observed_rate_counts_only_new_work_and_includes_waiting():
    p = Phase("samples", "Samples", total=100.0, completed=70.0, baseline=60.0, started=10.0)
    assert p.remaining(14.0) == (12.0, "observed")
    assert p.remaining(18.0) == (24.0, "observed")
    p.total = 110.0
    assert p.remaining(18.0) == (32.0, "observed")
    p.completed = 110.0
    assert p.remaining(18.0) == (None, "calibrating")
    p.finished = True
    assert p.remaining(18.0) == (0.0, "complete")


def test_previous_timing_scales_remaining_work_but_cannot_count_past_zero():
    p = Phase(
        "samples",
        "Samples",
        total=100.0,
        completed=50.0,
        baseline=50.0,
        prior=20.0,
        prior_units=100.0,
    )
    assert p.remaining(0.0) == (10.0, "previous_runs")
    p.started = 10.0
    assert p.remaining(15.0) == (5.0, "previous_runs")
    assert p.remaining(21.0) == (None, "calibrating")
    assert Phase("unknown", "Unknown").remaining(0) == (None, "calibrating")
    assert Phase("empty", "Empty", total=0.0).remaining(0) == (0.0, "complete")


def test_total_eta_never_excludes_an_unknown_future_phase():
    utc = datetime.now(UTC)
    active = Phase("samples", "Samples", total=100.0, completed=20.0, started=0.0)
    later = Phase("simulation", "Simulation")
    rows = (active.snapshot(10.0, utc), later.snapshot(10.0, utc))
    assert rows[0].eta.remaining_seconds == 40.0
    assert total_eta(rows, "running", utc).remaining_seconds is None
    later.prior = 20.0
    rows = (active.snapshot(10.0, utc), later.snapshot(10.0, utc))
    assert total_eta(rows, "running", utc).remaining_seconds == 60.0
    assert total_eta(rows, "running", utc).basis == "mixed"
    assert total_eta(rows, "failed", utc).basis == "unavailable"
    assert total_eta(rows, "completed", utc).remaining_seconds == 0.0


def test_eta_contract_requires_paired_finite_values():
    with pytest.raises(ValidationError, match="both be present"):
        Eta(remaining_seconds=10.0)
    with pytest.raises(ValidationError):
        Eta(remaining_seconds=float("nan"), estimated_finish_at=datetime.now(UTC))
    assert estimate(float("nan"), "observed", datetime.now(UTC)).remaining_seconds is None
    assert estimate(1e100, "observed", datetime.now(UTC)).remaining_seconds is None


def test_completed_timings_seed_next_start_and_are_scoped(tmp_path, clock):
    path, history = tmp_path / "job.progress.json", tmp_path / "timings.json"
    with ProgressJob(path, "same-context", history=history, interval=3600) as job:
        with phase("samples", "Samples", 100.0):
            clock[0] += 20.0
            job.advance(100.0)
    assert History(history).matching("same-context")[0].phases[0].elapsed_seconds == 20.0
    assert History(history).matching("different-context") == ()
    with ProgressJob(path, "same-context", history=history, interval=3600) as job:
        job.begin("samples", "Samples", 100.0)
        initial = job.snapshot()
        assert initial.eta.basis == "previous_runs"
        assert initial.eta.remaining_seconds == 20.0
        clock[0] += 2.0
        job.update(20.0)
        assert job.snapshot().eta.remaining_seconds == 8.0
        job.finish("samples")


def test_interrupt_resume_keeps_identity_but_uses_restored_work(tmp_path, clock):
    path = tmp_path / "job.progress.json"
    with pytest.raises(KeyboardInterrupt):
        with ProgressJob(path, "context", interval=3600) as first:
            first.begin("samples", "Samples", 100.0)
            first.advance(30.0)
            raise KeyboardInterrupt
    before = ProgressSnapshot.model_validate_json(path.read_text())
    assert before.status == "interrupted"
    assert before.eta.remaining_seconds is None
    assert before.phases[0].eta.remaining_seconds is None
    with ProgressJob(path, "context", resume=True, interval=3600) as resumed:
        resumed.begin("samples", "Samples", 100.0, completed=20.0)
        clock[0] += 5.0
        resumed.update(30.0)
        after = resumed.snapshot()
        assert after.job_id == before.job_id
        assert after.attempt_id != before.attempt_id
        assert after.sequence > before.sequence
        assert after.eta.remaining_seconds == 35.0
        resumed.finish("samples")


def test_stale_snapshots_stop_countdowns(tmp_path):
    with ProgressJob(tmp_path / "progress.json", "key", interval=3600) as job:
        job.begin("test", "Test", 10.0)
        snapshot = job.snapshot()
        assert snapshot.visible_eta(snapshot.updated_at) is not None
        expired = snapshot.updated_at + timedelta(seconds=snapshot.stale_after_seconds + 1)
        assert snapshot.visible_eta(expired) is None
        with pytest.raises(TypeError):
            snapshot.visible_eta(datetime.now())
    assert (
        ProgressSnapshot.model_validate_json(job.path.read_text()).visible_eta(datetime.now(UTC))
        is None
    )


def test_observer_failures_do_not_fail_the_job(tmp_path):
    def broken(snapshot):
        raise RuntimeError("observer unavailable")

    with ProgressJob(tmp_path / "progress.json", "key", callback=broken, interval=3600) as job:
        with phase("work", "Work", 1.0):
            job.advance()
        assert job.observer_errors >= 2
    result = ProgressSnapshot.model_validate_json(job.path.read_text())
    assert result.status == "completed"
    assert result.observer_errors > 0


def test_write_failure_is_observer_failure(tmp_path, monkeypatch):
    seen = []

    def broken(*args):
        raise OSError("disk unavailable")

    monkeypatch.setattr(progress, "atomic_snapshot", broken)
    with ProgressJob(tmp_path / "progress.json", "key", callback=seen.append, interval=3600) as job:
        with phase("work", "Work", 1.0):
            job.advance()
    assert seen[-1].status == "completed"
    assert seen[-1].observer_errors > 0


def test_heartbeat_file_callback_and_json_share_snapshots(tmp_path):
    delivered = []
    ready = threading.Event()
    stream = io.StringIO()

    def receive(snapshot):
        delivered.append(snapshot)
        if len(delivered) >= 2:
            ready.set()

    with ProgressJob(
        tmp_path / "progress.json",
        "key",
        callback=receive,
        stream=stream,
        json_events=True,
        interval=0.02,
    ) as job:
        job.begin("waiting", "Waiting", 1.0)
        assert ready.wait(2)
        job.finish("waiting")
    rows = [ProgressSnapshot.model_validate_json(row) for row in stream.getvalue().splitlines()]
    assert rows == delivered
    assert rows[-1] == ProgressSnapshot.model_validate_json(job.path.read_text())
    assert [r.sequence for r in rows] == sorted({r.sequence for r in rows})
    assert rows[-1].elapsed_seconds > rows[0].elapsed_seconds


def test_exclusive_progress_writer_and_invalid_files_are_refused(tmp_path):
    path = tmp_path / "progress.json"
    with ProgressJob(path, "key", interval=3600):
        with pytest.raises(BlockingIOError):
            with ProgressJob(path, "key", interval=3600):
                pass
    path.write_text('{"unrelated": true}')
    with pytest.raises(ValidationError):
        with ProgressJob(path, "key", interval=3600):
            pass
    assert json.loads(path.read_text()) == {"unrelated": True}


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf")])
def test_invalid_heartbeat_interval(tmp_path, interval):
    with pytest.raises(ValueError, match="positive"):
        ProgressJob(tmp_path / "progress.json", "key", interval=interval)


def test_terminal_uses_phase_estimate_without_claiming_a_total(tmp_path, clock):
    stream = io.StringIO()
    with pytest.raises(RuntimeError):
        with ProgressJob(tmp_path / "progress.json", "key", stream=stream, interval=3600) as job:
            job.plan("future", "Unknown future")
            job.begin("samples", "Samples", 100.0)
            clock[0] += 10.0
            job.update(20.0)
            job.publish(force=True)
            raise RuntimeError("failed acquisition")
    assert "phase ETA ~40s" in stream.getvalue()
    assert "total ETA calculating" in stream.getvalue()
    assert "Backtest failed" in stream.getvalue()
    assert "total ETA unavailable" in stream.getvalue()
    assert progress.current() is None


def test_invalid_timing_history_does_not_block_or_overwrite_it(tmp_path):
    history = tmp_path / "timings.json"
    history.write_text('{"unrelated": true}')
    with ProgressJob(tmp_path / "progress.json", "key", history=history, interval=3600) as job:
        with phase("work", "Work", 1.0, "phases"):
            pass
    assert json.loads(history.read_text()) == {"unrelated": True}
    assert job.observer_errors >= 2


def test_history_is_bounded_and_work_units_are_observed(tmp_path):
    from almanak.backtest.progress_types import TimingEntry

    history = History(tmp_path / "timings.json")
    for index in range(66):
        history.record(TimingEntry(key=str(index), phases=()))
    assert len(history.read().entries) == 64
    assert history.read().entries[0].key == "2"


def test_teardown_finishes_at_actual_work_not_at_its_deadline(tmp_path):
    with ProgressJob(tmp_path / "progress.json", "key", interval=3600) as job:
        job.plan("simulation", "Simulating", 100.0, "simulation_seconds")
        job.plan("teardown", "Teardown", None, "simulation_seconds", estimated=True)
        progress.simulation(100, 200, 800, 190, "running", 9)
        progress.simulation(100, 200, 800, 200, "teardown", 10)
        progress.simulation(100, 200, 800, 202, "finished", 11)
    row = next(p for p in job.snapshot().phases if p.phase_id == "teardown")
    assert row.completed_units == row.total_units == 2.0
    assert not row.total_is_estimate


def test_tty_updates_one_line_and_finishes_with_newline(tmp_path):
    class Terminal(io.StringIO):
        def isatty(self):
            return True

    stream = Terminal()
    with ProgressJob(tmp_path / "progress.json", "key", stream=stream, interval=3600) as job:
        with phase("work", "Work", 1.0, "phases"):
            job.clear_terminal()
    assert "\r\x1b[2K" in stream.getvalue()
    assert stream.getvalue().endswith("\n")


def test_composed_acquisition_keeps_native_counts_distinct_and_restores_scope(tmp_path):
    from almanak.backtest import progress

    with ProgressJob(tmp_path / "progress.json", "composed", interval=3600) as job:
        for connector, total in (("aave_v3", 2), ("uniswap_v3", 3)):
            with progress.phase_scope(connector):
                with progress.phase("preparation", "Native inputs", total):
                    progress.advance(total)
        with pytest.raises(RuntimeError):
            with progress.phase_scope("failed"):
                raise RuntimeError("component interrupted")
        with progress.phase("finalization", "Finalizing", 1):
            progress.advance()
        assert job.phases["aave_v3.preparation"].completed == 2
        assert job.phases["uniswap_v3.preparation"].completed == 3
        assert job.phases["finalization"].completed == 1
