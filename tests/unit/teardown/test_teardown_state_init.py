"""ALM-2705: TeardownStateManager._init_db retries under WAL contention.

Two compounding defects fixed:

1. ``TeardownStateManager._init_db()`` was the only schema-init method NOT
   wrapped in ``_with_retry``. Under WAL contention from co-tenant writers
   (gateway lifecycle store, accounting writers) on the same
   ``<workspace>/almanak_state.db``, a transient ``OperationalError`` could
   leave the singleton without a ``teardown_requests`` table. Subsequent
   ``create_request()`` calls then hit ``no such table: teardown_requests``.

2. ``IntentStrategy._check_teardown_request()`` swallowed every Exception with
   a broad ``except Exception``, hiding init failures as if they were the
   benign "no teardown row exists" path. Now narrowed: ``LocalPathError`` is
   benign (debug log), ``OperationalError`` is loud (ERROR log with a
   grep-able structured marker), other exceptions get a typed warning. In
   every failure case the runner still returns ``None`` from the request
   check — per CLAUDE.md "Teardown lane accounting boundary", failures must
   be loud + durable but must NEVER block the next risk-reducing iteration.

Schema-init idempotency: ``CREATE TABLE IF NOT EXISTS`` is a no-op on an
already-initialized DB, so retrying ``_init_db`` is always safe.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.framework.teardown import state_manager as state_manager_module
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownRequest,
)
from almanak.framework.teardown.state_manager import (
    TeardownStateManager,
    _with_retry,
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _has_teardown_requests_table(db_path: Path) -> bool:
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='teardown_requests'")
        return cursor.fetchone() is not None


def _make_request(strategy_id: str = "alm-2705-test") -> TeardownRequest:
    return TeardownRequest(
        strategy_id=strategy_id,
        mode=TeardownMode.SOFT,
        reason="alm-2705-test",
        requested_by="test",
    )


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    """Each test gets a fresh module-level singleton."""
    state_manager_module._default_manager = None
    yield
    state_manager_module._default_manager = None


@pytest.fixture(autouse=True)
def _short_retry_delays(monkeypatch: pytest.MonkeyPatch) -> None:
    """Speed up the retry loop so concurrency tests don't drag.

    The retry policy and attempt count are unchanged — only the *base sleep*
    is shrunk to keep tests under a second.
    """
    monkeypatch.setattr(state_manager_module, "_SQLITE_RETRY_BASE_DELAY_S", 0.001)


# -----------------------------------------------------------------------------
# Defect 1: _init_db now retries under WAL contention
# -----------------------------------------------------------------------------


class TestInitDbRetry:
    def test_init_db_succeeds_after_transient_operational_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Simulate the WAL-contention failure mode: first N _open_connection
        calls inside ``_init_db`` raise ``OperationalError('database is
        locked')``; the retry wrapper must keep trying until success.
        """
        db_path = tmp_path / "alm-2705.db"
        real_open = state_manager_module._open_connection
        call_count = {"n": 0}
        fail_first_n = 2  # < _SQLITE_RETRY_ATTEMPTS (5) so we eventually win

        def flaky_open(path):  # noqa: ANN001
            call_count["n"] += 1
            if call_count["n"] <= fail_first_n:
                raise sqlite3.OperationalError("database is locked")
            return real_open(path)

        with patch.object(state_manager_module, "_open_connection", side_effect=flaky_open):
            caplog.set_level(logging.WARNING, logger="almanak.framework.teardown.state_manager")
            mgr = TeardownStateManager(db_path=db_path)

        # Init succeeded -> table must exist on disk.
        assert _has_teardown_requests_table(db_path)
        assert call_count["n"] >= fail_first_n + 1

        # Manager is usable end-to-end (insert + read).
        mgr.create_request(_make_request())
        assert mgr.get_request("alm-2705-test") is not None

        # Retry attempts produced WARNING logs — useful for grep during
        # incident triage.
        retry_warnings = [
            r for r in caplog.records if "init_teardown_requests" in r.getMessage() and "retrying" in r.getMessage()
        ]
        assert len(retry_warnings) >= fail_first_n

    def test_init_db_propagates_after_exhausted_retries(self, tmp_path: Path) -> None:
        """Permanent OperationalError must propagate after exhausting the
        retry budget — no silent corruption of the singleton state."""
        db_path = tmp_path / "alm-2705-perm.db"

        def always_fail(path):  # noqa: ANN001
            raise sqlite3.OperationalError("database is locked")

        with patch.object(state_manager_module, "_open_connection", side_effect=always_fail):
            with pytest.raises(sqlite3.OperationalError, match="database is locked"):
                TeardownStateManager(db_path=db_path)

    def test_init_db_is_idempotent(self, tmp_path: Path) -> None:
        """Calling _init_db repeatedly on an already-initialized DB is a
        no-op — CREATE TABLE IF NOT EXISTS is naturally idempotent and the
        retry wrapper does not change that.
        """
        db_path = tmp_path / "alm-2705-idem.db"
        mgr = TeardownStateManager(db_path=db_path)

        # Insert a row, then re-init: row must still be present.
        mgr.create_request(_make_request("idempotent-strat"))
        mgr._init_db()
        mgr._init_db()
        mgr._init_db()

        assert mgr.get_request("idempotent-strat") is not None
        assert _has_teardown_requests_table(db_path)

    def test_init_db_under_real_writer_contention(self, tmp_path: Path) -> None:
        """End-to-end concurrency: spawn a background writer holding a
        transaction on the same DB while ``TeardownStateManager`` initializes.
        With WAL + busy_timeout + retry, init must succeed and the schema
        must be present.

        This is the closest test we can run to the AlmanakCode workspace
        repro without spinning up the full gateway.
        """
        db_path = tmp_path / "alm-2705-concurrent.db"

        # Pre-create the DB with WAL on so the contender has somewhere to
        # write — _init_db's CREATE TABLE IF NOT EXISTS is the operation
        # under contention.
        init_conn = sqlite3.connect(str(db_path))
        init_conn.execute("PRAGMA journal_mode = WAL")
        init_conn.execute("CREATE TABLE IF NOT EXISTS contender (id INTEGER)")
        init_conn.commit()
        init_conn.close()

        contender_holding_tx = threading.Event()
        contender_should_release = threading.Event()
        contender_done = threading.Event()

        def contender() -> None:
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            try:
                conn.execute("PRAGMA busy_timeout = 1000")
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("INSERT INTO contender (id) VALUES (1)")
                contender_holding_tx.set()
                # Hold the write lock briefly to force the init path to
                # bounce off OperationalError at least once.
                contender_should_release.wait(timeout=5.0)
                conn.commit()
            finally:
                conn.close()
                contender_done.set()

        t = threading.Thread(target=contender, daemon=True)
        t.start()
        assert contender_holding_tx.wait(timeout=5.0), "contender failed to grab BEGIN IMMEDIATE"

        # Release the contender after a brief hold so retries can succeed.
        def releaser() -> None:
            time.sleep(0.05)
            contender_should_release.set()

        threading.Thread(target=releaser, daemon=True).start()

        mgr = TeardownStateManager(db_path=db_path)
        # Assert thread completion — without these asserts, a contender that
        # stalls past the timeout would silently let the test pass and mask
        # lock-handling regressions.
        assert contender_done.wait(timeout=5.0), "contender thread did not finish in time"
        t.join(timeout=5.0)
        assert not t.is_alive(), "contender thread did not exit cleanly"

        assert _has_teardown_requests_table(db_path)
        # Manager fully usable after contention clears.
        mgr.create_request(_make_request("concurrent-strat"))
        assert mgr.get_request("concurrent-strat") is not None


# -----------------------------------------------------------------------------
# Defect 2: should_teardown / _check_teardown_request narrow exceptions
# -----------------------------------------------------------------------------


class _StubStrategy:
    """Minimal stand-in exposing the attributes _check_teardown_request reads.

    We bind the real method off ``IntentStrategy`` so we exercise the actual
    code path, but skip importing the full IntentStrategy machinery (which
    pulls in heavy dependencies).
    """

    STRATEGY_NAME = "alm-2705-stub"

    def __init__(self, strategy_id: str | None = "alm-2705-stub") -> None:
        self._strategy_id = strategy_id


def _check_teardown_request_for(stub: _StubStrategy):
    """Invoke the unbound IntentStrategy._check_teardown_request against a stub."""
    from almanak.framework.strategies.intent_strategy import IntentStrategy

    return IntentStrategy._check_teardown_request(stub)


def _acknowledge_teardown_request_for(stub: _StubStrategy) -> bool:
    """Invoke the unbound IntentStrategy.acknowledge_teardown_request against a stub."""
    from almanak.framework.strategies.intent_strategy import IntentStrategy

    return IntentStrategy.acknowledge_teardown_request(stub)


def _should_teardown_for(stub: _StubStrategy) -> bool:
    """Invoke the full ``should_teardown`` path. Binds ``_check_teardown_request``
    onto the stub so the unbound delegation in ``should_teardown`` works.
    """
    from almanak.framework.strategies.intent_strategy import IntentStrategy

    # Bind the bound method so should_teardown's `self._check_teardown_request()`
    # resolves on the stub instance.
    stub._check_teardown_request = lambda: IntentStrategy._check_teardown_request(stub)  # type: ignore[attr-defined]
    return IntentStrategy.should_teardown(stub)


@pytest.fixture
def _local_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force ``is_hosted()`` False so the real teardown channel is exercised."""
    monkeypatch.delenv("AGENT_ID", raising=False)


@pytest.fixture
def _strategy_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the teardown state manager at an isolated temp DB."""
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "alm-2705-strat.db"))
    monkeypatch.delenv("ALMANAK_TEARDOWN_STATE_DB", raising=False)
    return tmp_path / "alm-2705-strat.db"


class TestShouldTeardownExceptionNarrowing:
    def test_benign_no_request_returns_false_without_log_noise(
        self,
        _local_mode: None,
        _strategy_db: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Regression: the success path (no teardown row) is unchanged. No
        ERROR / WARNING log noise."""
        caplog.set_level(logging.DEBUG, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()

        # Drains the lazy import of get_teardown_state_manager + clean init.
        result = _should_teardown_for(stub)

        assert result is False
        # No loud logs at WARNING+ from the teardown-check code path.
        loud = [
            r
            for r in caplog.records
            if r.levelno >= logging.WARNING
            and ("teardown.check_request" in r.getMessage() or "Error checking teardown request" in r.getMessage())
        ]
        assert loud == [], f"unexpected loud logs on benign path: {[r.getMessage() for r in loud]}"

    def test_init_failure_logs_loud_error_and_does_not_block_runner(
        self,
        _local_mode: None,
        _strategy_db: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Permanent init failure (e.g. read-only fs) must:
        1. NOT silently return False under a debug log (pre-fix behaviour).
        2. Emit a grep-able ``teardown.check_request_failed`` ERROR.
        3. Still return None / False so the runner keeps making
           risk-reducing progress (CLAUDE.md teardown lane contract).
        """

        # Force every _open_connection inside _init_db to raise a permanent
        # OperationalError. The retry wrapper exhausts and propagates the
        # error up to ``_check_teardown_request``, which must classify it
        # as the loud path.
        def always_fail(path):  # noqa: ANN001
            raise sqlite3.OperationalError("attempt to write a readonly database")

        caplog.set_level(logging.ERROR, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()

        with patch.object(state_manager_module, "_open_connection", side_effect=always_fail):
            result = _check_teardown_request_for(stub)
            should = _should_teardown_for(stub)

        # Loud, durable, but not blocking.
        assert result is None
        assert should is False

        loud_errors = [r for r in caplog.records if "teardown.check_request_failed" in r.getMessage()]
        assert loud_errors, "expected ERROR-level structured log marker for init failure"
        assert any(r.levelno == logging.ERROR for r in loud_errors)

    def test_missing_table_operational_error_surfaces_loud(
        self,
        _local_mode: None,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Direct repro of the ALM-2705 production symptom: the singleton is
        already constructed and cached, but the underlying file's schema is
        missing the ``teardown_requests`` table (e.g. another process truncated
        it, or the DB was created without our schema). The next iteration's
        ``get_active_request`` raises OperationalError. This must NOT be
        swallowed as 'no request'.
        """
        db_path = tmp_path / "alm-2705-no-table.db"
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))
        # Hermetic env: clear ALMANAK_TEARDOWN_STATE_DB so path resolution
        # never bypasses the db_path we're constructing under tmp_path.
        monkeypatch.delenv("ALMANAK_TEARDOWN_STATE_DB", raising=False)

        # Build a manager normally, then drop the table to simulate the
        # production failure shape.
        mgr = TeardownStateManager(db_path=db_path)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("DROP TABLE teardown_requests")
            conn.commit()
        # Wire it as the singleton so _check_teardown_request picks it up.
        state_manager_module._default_manager = mgr

        caplog.set_level(logging.ERROR, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()
        result = _check_teardown_request_for(stub)

        assert result is None  # runner keeps moving
        loud = [r for r in caplog.records if "teardown.check_request_failed" in r.getMessage()]
        assert loud, "expected loud structured log for missing-table OperationalError"

    def test_unexpected_exception_logs_typed_warning(
        self,
        _local_mode: None,
        _strategy_db: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A non-OperationalError, non-LocalPathError failure must be logged
        with the exception type so future incidents are triagable from logs
        alone. This proves we did NOT trade one silent bug for another.
        """

        class WeirdError(RuntimeError):
            pass

        def bad_open(path):  # noqa: ANN001
            raise WeirdError("unexpected channel failure")

        caplog.set_level(logging.WARNING, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()

        with patch.object(state_manager_module, "_open_connection", side_effect=bad_open):
            result = _check_teardown_request_for(stub)

        assert result is None
        warnings_ = [r for r in caplog.records if "teardown.check_request_unexpected_error" in r.getMessage()]
        assert warnings_, "expected typed warning for unexpected exception"
        assert any("WeirdError" in r.getMessage() for r in warnings_)

    def test_local_path_error_is_benign_debug_only(
        self,
        _local_mode: None,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A LocalPathError (no strategy folder resolved) must NOT log loud —
        it's a configuration shape, not a bug class.
        """
        # Wipe every signal _resolve_db_path could pick up.
        monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        monkeypatch.delenv("ALMANAK_TEARDOWN_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)

        from almanak.framework.local_paths import LocalPathError

        # Force the resolver to raise LocalPathError deterministically.
        def boom(_db_path):  # noqa: ANN001
            raise LocalPathError("no strategy folder resolved")

        monkeypatch.setattr(TeardownStateManager, "_resolve_db_path", staticmethod(boom))

        caplog.set_level(logging.DEBUG, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()
        result = _check_teardown_request_for(stub)

        assert result is None
        # No ERROR / WARNING — only DEBUG is acceptable for this benign case.
        loud = [
            r for r in caplog.records if r.levelno >= logging.WARNING and "teardown.check_request" in r.getMessage()
        ]
        assert loud == [], f"LocalPathError must not log loud: {[r.getMessage() for r in loud]}"


# -----------------------------------------------------------------------------
# Defect 2 (mirror): acknowledge_teardown_request shares the same narrowing
# contract as _check_teardown_request. Cover the same four branches so the
# CRAP gate sees coverage for both paths and a future contributor doesn't
# silently re-broaden the exception handler.
# -----------------------------------------------------------------------------


class TestAcknowledgeTeardownRequestExceptionNarrowing:
    def test_hosted_mode_short_circuits_without_db_access(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Hosted deployments do not use the local teardown channel — the
        method must early-exit without touching the DB or emitting logs.
        """
        monkeypatch.setenv("AGENT_ID", "alm-2705-hosted-stub")
        caplog.set_level(logging.DEBUG, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()

        result = _acknowledge_teardown_request_for(stub)

        assert result is False
        assert not [r for r in caplog.records if "teardown.ack" in r.getMessage()]

    def test_returns_true_when_active_request_acknowledged(
        self,
        _local_mode: None,
        _strategy_db: Path,
    ) -> None:
        """Happy path: an active request exists → ack returns True."""
        from almanak.framework.teardown import get_teardown_state_manager

        mgr = get_teardown_state_manager()
        mgr.create_request(_make_request(strategy_id="alm-2705-stub"))

        stub = _StubStrategy()
        result = _acknowledge_teardown_request_for(stub)

        assert result is True

    def test_returns_false_when_no_active_request(
        self,
        _local_mode: None,
        _strategy_db: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Happy path: no active request → return False, no log noise."""
        caplog.set_level(logging.DEBUG, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()

        result = _acknowledge_teardown_request_for(stub)

        assert result is False
        loud = [
            r
            for r in caplog.records
            if r.levelno >= logging.WARNING and "teardown.ack" in r.getMessage()
        ]
        assert loud == [], f"unexpected loud logs on benign path: {[r.getMessage() for r in loud]}"

    def test_local_path_error_is_benign_debug_only(
        self,
        _local_mode: None,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """LocalPathError is a configuration shape, not a bug — debug only."""
        monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        monkeypatch.delenv("ALMANAK_TEARDOWN_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)

        from almanak.framework.local_paths import LocalPathError

        def boom(_db_path):  # noqa: ANN001
            raise LocalPathError("no strategy folder resolved")

        monkeypatch.setattr(TeardownStateManager, "_resolve_db_path", staticmethod(boom))

        caplog.set_level(logging.DEBUG, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()
        result = _acknowledge_teardown_request_for(stub)

        assert result is False
        loud = [
            r
            for r in caplog.records
            if r.levelno >= logging.WARNING and "teardown.ack" in r.getMessage()
        ]
        assert loud == [], f"LocalPathError must not log loud: {[r.getMessage() for r in loud]}"

    def test_operational_error_logs_loud_structured_marker(
        self,
        _local_mode: None,
        _strategy_db: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """sqlite3.OperationalError on the ack path must surface as the
        grep-able ``teardown.ack_request_failed`` ERROR with the full
        ``strategy_id`` / ``strategy_class`` / ``error`` triage tuple.
        """

        def always_fail(path):  # noqa: ANN001
            raise sqlite3.OperationalError("attempt to write a readonly database")

        caplog.set_level(logging.ERROR, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()

        with patch.object(state_manager_module, "_open_connection", side_effect=always_fail):
            result = _acknowledge_teardown_request_for(stub)

        assert result is False
        loud = [r for r in caplog.records if "teardown.ack_request_failed" in r.getMessage()]
        assert loud, "expected ERROR-level structured log marker for ack-path init failure"
        assert any(r.levelno == logging.ERROR for r in loud)
        # Triage tuple must be present so future incidents are grep-able.
        assert any("strategy_id=" in r.getMessage() for r in loud)
        assert any("strategy_class=" in r.getMessage() for r in loud)

    def test_unexpected_exception_logs_typed_warning(
        self,
        _local_mode: None,
        _strategy_db: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A non-OperationalError, non-LocalPathError failure must surface
        with the exception type so future incidents are triagable from logs
        alone — proving we did not trade one silent bug for another on the
        ack path.
        """

        class WeirdError(RuntimeError):
            pass

        def bad_open(path):  # noqa: ANN001
            raise WeirdError("unexpected ack channel failure")

        caplog.set_level(logging.WARNING, logger="almanak.framework.strategies.intent_strategy")
        stub = _StubStrategy()

        with patch.object(state_manager_module, "_open_connection", side_effect=bad_open):
            result = _acknowledge_teardown_request_for(stub)

        assert result is False
        warnings_ = [r for r in caplog.records if "teardown.ack_request_unexpected_error" in r.getMessage()]
        assert warnings_, "expected typed warning for unexpected ack exception"
        assert any("WeirdError" in r.getMessage() for r in warnings_)


# -----------------------------------------------------------------------------
# _with_retry sanity (covers the wrapper itself for the new init path)
# -----------------------------------------------------------------------------


class TestWithRetryWrapperBehaviour:
    def test_with_retry_returns_value_when_first_attempt_succeeds(self) -> None:
        result = _with_retry(lambda: 42, description="alm-2705-sanity")
        assert result == 42

    def test_with_retry_eventually_succeeds(self) -> None:
        attempts = {"n": 0}

        def op() -> str:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise sqlite3.OperationalError("database is locked")
            return "ok"

        with patch.object(state_manager_module, "_SQLITE_RETRY_BASE_DELAY_S", 0.001):
            assert _with_retry(op, description="alm-2705-sanity") == "ok"
        assert attempts["n"] == 3
