"""VIB-5778: honest teardown-failure accounting (in-process, no schema change).

The PM Experiment 17 incident: ``generate_teardown_intents`` raised, the
generation-exception path called ``mark_failed`` with no counts, and — because
``mark_started`` (the only setter of ``positions_total`` / ``started_at``) never
ran — the row kept its 0-defaults. The CLI then printed a success-shaped
``positions_closed=0, positions_failed=0, total=0`` for a failure that stranded
a live position, and dropped the persisted ``error_message`` entirely.

These tests pin the three honesty guarantees (UNKNOWN is not zero):

1. ``error_message`` round-trips onto :class:`TeardownRequest` (model + SQLite
   read path) so every render site can surface it.
2. :attr:`TeardownRequest.counts_unmeasured` — read-side UNKNOWN inference: a
   FAILED row whose ``started_at IS NULL`` has unmeasured counts.
3. Best-effort enumeration on the generation-exception path
   (``runner_teardown._failure_position_counts``) yields real position-level
   counts when enumerable and ``(None, None)`` (preserve / never a fabricated
   zero) when not — isolated so it can never mask the original exception.

The persisted tri-state UNKNOWN contract, proto changes, and schema/migration
work are OUT OF SCOPE here (split to VIB-5792).
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from almanak.framework.runner import runner_teardown
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownRequest,
    TeardownStatus,
)
from almanak.framework.teardown.state_manager import TeardownStateManager


def _make_request(deployment_id: str = "S") -> TeardownRequest:
    return TeardownRequest(deployment_id=deployment_id, mode=TeardownMode.SOFT)


# ---------------------------------------------------------------------------
# 2. counts_unmeasured — read-side UNKNOWN inference
# ---------------------------------------------------------------------------
class TestCountsUnmeasured:
    def test_failed_without_started_at_is_unmeasured(self) -> None:
        """FAILED before enumeration ran (started_at IS NULL) → counts unknown."""
        req = _make_request()
        req.status = TeardownStatus.FAILED
        req.started_at = None
        assert req.counts_unmeasured is True

    def test_failed_with_started_at_is_measured(self) -> None:
        """A FAILED teardown that DID start carries real counts — never unknown."""
        req = _make_request()
        req.status = TeardownStatus.FAILED
        req.started_at = datetime.now(UTC)
        assert req.counts_unmeasured is False

    def test_non_failed_is_never_unmeasured(self) -> None:
        """Only FAILED rows infer UNKNOWN — a PENDING / COMPLETED row does not."""
        for status in (
            TeardownStatus.PENDING,
            TeardownStatus.EXECUTING,
            TeardownStatus.COMPLETED,
            TeardownStatus.CANCELLED,
        ):
            req = _make_request()
            req.status = status
            req.started_at = None
            assert req.counts_unmeasured is False, status


# ---------------------------------------------------------------------------
# 1. error_message round-trips onto the dataclass + SQLite read path
# ---------------------------------------------------------------------------
class TestErrorMessageSurfacing:
    def test_to_dict_from_dict_round_trip(self) -> None:
        req = _make_request()
        req.error_message = "generate_teardown_intents raised HealthUnavailableError"
        restored = TeardownRequest.from_dict(req.to_dict())
        assert restored.error_message == req.error_message

    def test_from_dict_defaults_to_none_when_absent(self) -> None:
        """Legacy payloads without the key decode to None (unset, not '')."""
        data = _make_request().to_dict()
        data.pop("error_message", None)
        assert TeardownRequest.from_dict(data).error_message is None

    def test_mark_failed_persists_and_reads_back_error_message(self, tmp_path: Path) -> None:
        """The SQLite read path (``_row_to_request``) now surfaces the persisted
        error_message onto the dataclass so the CLI can print it."""
        state = TeardownStateManager(db_path=str(tmp_path / "td.db"))
        state.create_request(_make_request())
        returned = state.mark_failed("S", error="boom: position stranded")
        # Returned object is consistent with the row.
        assert returned is not None
        assert returned.error_message == "boom: position stranded"
        # And a fresh read surfaces it too.
        reread = state.get_request("S")
        assert reread is not None
        assert reread.error_message == "boom: position stranded"

    def test_generation_exception_row_reads_unknown_and_error(self, tmp_path: Path) -> None:
        """End-to-end field-incident shape: create → mark_failed (no counts, no
        mark_started) → the row is FAILED, started_at IS NULL, counts unmeasured,
        and the error_message is present."""
        db_path = tmp_path / "td.db"
        state = TeardownStateManager(db_path=str(db_path))
        state.create_request(_make_request())
        state.mark_failed("S", error="HealthUnavailableError")
        req = state.get_request("S")
        assert req is not None
        assert req.status == TeardownStatus.FAILED
        assert req.started_at is None
        assert req.counts_unmeasured is True
        assert req.error_message == "HealthUnavailableError"
        # The raw columns are still the 0-defaults — the honesty is the read-side
        # inference, not a written sentinel (no schema change; VIB-5792 owns that).
        with sqlite3.connect(str(db_path)) as c:
            row = c.execute(
                "SELECT positions_total, positions_closed, positions_failed FROM "
                "teardown_requests WHERE deployment_id = ?",
                ("S",),
            ).fetchone()
        assert row == (0, 0, 0)

    def test_started_failure_renders_real_counts_not_unknown(self, tmp_path: Path) -> None:
        """FAILED after mark_started + real counts → measured, never unknown."""
        state = TeardownStateManager(db_path=str(tmp_path / "td.db"))
        state.create_request(_make_request())
        state.mark_started("S", total_positions=3)
        req = state.mark_failed("S", error="reverted", positions_closed=1, positions_failed=2)
        assert req is not None
        assert req.counts_unmeasured is False
        assert (req.positions_closed, req.positions_failed, req.positions_total) == (1, 2, 3)

    def test_started_failure_measured_zero_is_measured(self, tmp_path: Path) -> None:
        """Empty != Zero, the other direction: a started FAILED teardown with a
        genuine measured 0 is NOT unknown — it renders 0."""
        state = TeardownStateManager(db_path=str(tmp_path / "td.db"))
        state.create_request(_make_request())
        state.mark_started("S", total_positions=2)
        req = state.mark_failed("S", error="all reverted", positions_closed=0, positions_failed=0)
        assert req is not None
        assert req.counts_unmeasured is False


# ---------------------------------------------------------------------------
# 3. Best-effort enumeration on the generation-exception path
# ---------------------------------------------------------------------------
class _FakeSummary:
    def __init__(self, n: int) -> None:
        self.positions = [object() for _ in range(n)]


@pytest.mark.asyncio
class TestFailurePositionCounts:
    async def test_enumerable_returns_zero_closed_and_open_count(self, monkeypatch) -> None:
        """When positions ARE enumerable: nothing closed (measured 0), every open
        position is a failed-to-close position."""

        async def _fake(_strategy):  # noqa: ANN001, ANN202
            return _FakeSummary(3)

        monkeypatch.setattr(
            "almanak.framework.teardown.registry_enumeration.resolve_open_positions_with_registry",
            _fake,
        )
        assert await runner_teardown._failure_position_counts(object()) == (0, 3)

    async def test_unreadable_set_returns_none_pair(self, monkeypatch) -> None:
        """When enumeration cannot read the set, pass nothing (preserve) — NEVER a
        fabricated 0/0 — and let the read-side unknown inference render it."""

        async def _boom(_strategy):  # noqa: ANN001, ANN202
            raise RuntimeError("enumeration backend down")

        monkeypatch.setattr(
            "almanak.framework.teardown.registry_enumeration.resolve_open_positions_with_registry",
            _boom,
        )
        # _count_open_positions swallows the error to None; helper maps to (None, None).
        assert await runner_teardown._failure_position_counts(object()) == (None, None)

    async def test_helper_contains_its_own_error(self, monkeypatch) -> None:
        """The honesty step is defensively wrapped: even if the count helper
        itself raises, ``_failure_position_counts`` returns (None, None) rather
        than propagating and masking the original teardown exception."""

        async def _boom(_strategy):  # noqa: ANN001, ANN202
            raise RuntimeError("unexpected enumeration fault")

        monkeypatch.setattr(
            "almanak.framework.runner.runner_teardown._count_open_positions",
            _boom,
        )
        assert await runner_teardown._failure_position_counts(object()) == (None, None)


# ---------------------------------------------------------------------------
# Hosted parity — Postgres _row_to_request must surface error_message too
# (VIB-5778). Skipped cleanly on checkouts without the optional plugin; runs
# in hosted / mirror CI where almanak_platform is installed.
# ---------------------------------------------------------------------------
try:
    from almanak_platform.teardown_store import PostgresTeardownStateManager

    _POSTGRES_AVAILABLE = True
except ImportError:  # pragma: no cover - only in mirror/hosted builds
    PostgresTeardownStateManager = None  # type: ignore[assignment,misc]
    _POSTGRES_AVAILABLE = False


@pytest.mark.skipif(not _POSTGRES_AVAILABLE, reason="platform-plugins (Postgres) not installed")
def test_postgres_row_to_request_surfaces_error_message() -> None:
    """The hosted read path must map the persisted error_message onto the
    dataclass, symmetric with the SQLite _row_to_request — otherwise every
    hosted status/list/--wait response drops the failure reason."""
    row = {
        "deployment_id": "S",
        "mode": "SOFT",
        "asset_policy": "target_token",
        "target_token": "USDC",
        "reason": None,
        "requested_at": datetime.now(UTC),
        "requested_by": None,
        "status": "failed",
        "acknowledged_at": None,
        "started_at": None,
        "completed_at": datetime.now(UTC),
        "current_phase": None,
        "positions_total": 0,
        "positions_closed": 0,
        "positions_failed": 0,
        "error_message": "generate_teardown_intents raised HealthUnavailableError",
        "cancel_requested": False,
        "cancel_deadline": None,
    }
    req = PostgresTeardownStateManager._row_to_request(row)
    assert req.error_message == "generate_teardown_intents raised HealthUnavailableError"
    # And the read-side UNKNOWN inference is identical on the hosted path.
    assert req.counts_unmeasured is True
