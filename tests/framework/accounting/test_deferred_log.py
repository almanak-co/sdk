"""Tests for ``almanak.framework.accounting.deferred_log`` (VIB-3773 Phase 1).

The deferred-write log is a best-effort durable backstop for accounting
writes that failed during teardown. Tests cover:

* T6: atomic concurrent appends (no torn lines under multi-process writes).
* Local-mode JSON-line round-trip.
* Hosted-mode structured-log emission (no JSONL file).
* Last-resort sanitisation: secret-shaped keys redacted in ``extra``.
* Never-raises invariant: serialisation failures don't propagate.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
from pathlib import Path

import pytest

from almanak.framework.accounting import deferred_log
from almanak.framework.accounting.deferred_log import (
    DEFERRED_LOG_FILENAME,
    DeferredWrite,
    append,
    append_now,
)

# ---------------------------------------------------------------------------
# Local-mode helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def local_db_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Pin the local DB resolver to ``tmp_path`` for the test.

    Sets ``ALMANAK_STATE_DB`` and unsets the hosted-mode env so
    :func:`local_db_path` returns a sibling under ``tmp_path``.
    """
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))
    return tmp_path


# ---------------------------------------------------------------------------
# DeferredWrite.now / to_json_line
# ---------------------------------------------------------------------------


def test_deferred_write_now_stamps_iso_utc():
    rec = DeferredWrite.now(
        kind="ledger",
        deployment_id="d1",
        cycle_id="teardown-abc",
    )
    assert rec.kind == "ledger"
    assert rec.deployment_id == "d1"
    assert rec.cycle_id == "teardown-abc"
    # ISO-8601 with timezone (parses cleanly).
    from datetime import datetime

    parsed = datetime.fromisoformat(rec.ts)
    assert parsed.tzinfo is not None


def test_to_json_line_round_trips():
    rec = DeferredWrite.now(
        kind="outbox",
        deployment_id="d1",
        cycle_id="teardown-xyz",
        intent_type="LP_CLOSE",
        tx_hash="0xabc",
        ledger_entry_id="le-1",
        error="boom",
        extra={"slippage_bps": 25},
    )
    line = rec.to_json_line()
    assert line.endswith("\n")
    payload = json.loads(line)
    assert payload["kind"] == "outbox"
    assert payload["intent_type"] == "LP_CLOSE"
    assert payload["error"] == "boom"
    assert payload["extra"] == {"slippage_bps": 25}


def test_error_field_is_truncated():
    rec = DeferredWrite.now(
        kind="ledger",
        deployment_id="d1",
        cycle_id="teardown-zzz",
        error="x" * 5000,
    )
    payload = json.loads(rec.to_json_line())
    assert len(payload["error"]) == 1000  # _ERROR_TRUNCATE


def test_extra_redacts_obviously_secret_keys():
    rec = DeferredWrite.now(
        kind="ledger",
        deployment_id="d1",
        cycle_id="teardown-1",
        extra={
            "private_key": "0xdeadbeef",
            "AUTH_TOKEN": "tok",
            "intent_id": "ok",
        },
    )
    payload = json.loads(rec.to_json_line())
    extra = payload["extra"]
    assert extra["private_key"] == "[REDACTED]"
    assert extra["AUTH_TOKEN"] == "[REDACTED]"
    assert extra["intent_id"] == "ok"  # unrelated key untouched


# ---------------------------------------------------------------------------
# append() — local mode writes JSONL
# ---------------------------------------------------------------------------


def test_append_local_writes_jsonl(local_db_dir: Path):
    ok = append_now(
        kind="ledger",
        deployment_id="d1",
        cycle_id="teardown-1",
        intent_type="LP_CLOSE",
        tx_hash="0x1",
        error="save_ledger_entry returned False",
    )
    assert ok is True
    log_path = local_db_dir / DEFERRED_LOG_FILENAME
    assert log_path.exists()
    lines = log_path.read_text().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["kind"] == "ledger"
    assert payload["tx_hash"] == "0x1"
    assert payload["intent_type"] == "LP_CLOSE"


def test_append_local_appends_not_overwrites(local_db_dir: Path):
    for i in range(5):
        append_now(
            kind="outbox",
            deployment_id="d1",
            cycle_id="teardown-1",
            intent_type="SWAP",
            tx_hash=f"0x{i}",
        )
    log_path = local_db_dir / DEFERRED_LOG_FILENAME
    lines = log_path.read_text().splitlines()
    assert len(lines) == 5
    # Each line must be a complete, parseable JSON object — no torn writes.
    for line in lines:
        json.loads(line)


# ---------------------------------------------------------------------------
# T6 — concurrent appends from multiple processes produce well-formed JSONL
# ---------------------------------------------------------------------------


def _worker_append(state_db_path: str, kind_prefix: str, count: int) -> None:
    """Run inside a child process: append ``count`` records with unique keys.

    Re-imports inside the child because multiprocessing on some platforms
    won't carry monkeypatched env reliably; we set the env var explicitly.
    """
    os.environ["ALMANAK_STATE_DB"] = state_db_path
    os.environ.pop("ALMANAK_IS_HOSTED", None)
    os.environ.pop("ALMANAK_DEPLOYMENT_ID", None)

    # Reload modules to pick up the env (defensive).
    from importlib import reload

    from almanak.framework import local_paths

    reload(local_paths)
    from almanak.framework.accounting import deferred_log as dl

    reload(dl)

    for i in range(count):
        dl.append_now(
            kind=f"{kind_prefix}-{i}",
            deployment_id="dep",
            cycle_id="teardown-concurrent",
            intent_type="SWAP",
            tx_hash=f"0x{kind_prefix}{i:04d}",
            error="concurrent test",
            extra={"worker": kind_prefix, "i": i},
        )


def test_concurrent_appends_produce_no_torn_lines(local_db_dir: Path):
    """Spawn N workers, each appending K records. Every resulting line must
    be a complete, JSON-parseable object — i.e. POSIX O_APPEND semantics
    held under contention.
    """
    state_db = str(local_db_dir / "state.db")
    n_workers = 4
    per_worker = 50

    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=_worker_append, args=(state_db, f"w{i}", per_worker)) for i in range(n_workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
        assert p.exitcode == 0, f"worker died: exitcode={p.exitcode}"

    log_path = local_db_dir / DEFERRED_LOG_FILENAME
    lines = log_path.read_text().splitlines()
    assert len(lines) == n_workers * per_worker

    # Every line parses; no torn writes.
    cycle_ids = set()
    for line in lines:
        payload = json.loads(line)
        cycle_ids.add(payload["cycle_id"])
        assert payload["cycle_id"] == "teardown-concurrent"
        assert payload["error"] == "concurrent test"
    assert cycle_ids == {"teardown-concurrent"}


# ---------------------------------------------------------------------------
# Hosted-mode behaviour
# ---------------------------------------------------------------------------


def test_append_hosted_emits_structured_log_no_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    """In hosted mode (ALMANAK_IS_HOSTED set), no JSONL file is created —
    instead a WARNING log line is emitted with ``event=accounting_deferred``.
    """
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-xyz")
    monkeypatch.setenv("ALMANAK_GATEWAY_DATABASE_URL", "postgresql://fake")
    monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "tok")
    monkeypatch.delenv("ALMANAK_GATEWAY_ALLOW_INSECURE", raising=False)
    monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)

    caplog.set_level("WARNING", logger="almanak.framework.accounting.deferred_log")

    ok = append_now(
        kind="ledger",
        deployment_id="d1",
        cycle_id="teardown-hosted",
        intent_type="REPAY",
        error="ledger fail",
    )
    assert ok is True

    # No JSONL file should exist anywhere under tmp_path or cwd-like folders.
    for f in tmp_path.rglob(DEFERRED_LOG_FILENAME):
        pytest.fail(f"unexpected JSONL file in hosted mode: {f}")

    matched = [r for r in caplog.records if "accounting_deferred" in r.getMessage()]
    assert matched, "no accounting_deferred log record emitted in hosted mode"
    rec = matched[0]
    assert getattr(rec, "event", None) == "accounting_deferred"
    assert getattr(rec, "deferred_kind", None) == "ledger"
    assert getattr(rec, "deployment_id", None) == "d1"
    assert getattr(rec, "cycle_id", None) == "teardown-hosted"


# ---------------------------------------------------------------------------
# Never-raises invariant
# ---------------------------------------------------------------------------


def test_append_never_raises_on_serialise_failure(monkeypatch: pytest.MonkeyPatch, local_db_dir: Path):
    """A pathological ``DeferredWrite`` whose to_json_line raises must not
    propagate the exception out of :func:`append`.
    """

    class BadRecord:
        kind = "ledger"
        deployment_id = "d1"
        cycle_id = "teardown-bad"
        intent_type = None
        tx_hash = None
        error = ""

        def to_json_line(self) -> str:  # noqa: D401 — test-only
            raise RuntimeError("boom")

    # Should NOT raise — silently returns False.
    result = append(BadRecord())  # type: ignore[arg-type]
    assert result is False


def test_append_falls_back_to_hosted_when_local_unwritable(
    monkeypatch: pytest.MonkeyPatch,
    local_db_dir: Path,
    caplog: pytest.LogCaptureFixture,
):
    """If the local target raises an OSError mid-write, we fall back to the
    hosted structured-log path rather than dying.
    """

    def _broken_append(path: Path, line: str) -> bool:
        return False  # simulate write failure

    monkeypatch.setattr(deferred_log, "_append_local", _broken_append)
    caplog.set_level("WARNING", logger="almanak.framework.accounting.deferred_log")

    ok = append_now(
        kind="snapshot",
        deployment_id="d1",
        cycle_id="teardown-fallback",
        error="forced",
    )
    # Hosted-emit succeeds (logger always returns).
    assert ok is True
    # No file should have been written successfully.
    assert not (local_db_dir / DEFERRED_LOG_FILENAME).exists()
    matched = [r for r in caplog.records if "accounting_deferred" in r.getMessage()]
    assert matched, "fallback log line missing"
