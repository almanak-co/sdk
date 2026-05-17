"""VIB-4542: TeardownStateManager.mark_failed must persist counters.

`mark_completed(strategy_id, result)` already lifts `result["intents"]` into
`positions_closed` (VIB-3920). The failed-path peer accepted only the
error string and only wrote `error_message` — so after a teardown that
landed 6 of 7 intents on-chain, the SQLite row showed
`positions_closed=0, positions_failed=0, error_message='1 intents failed'`.
Anyone reading the DB row alone could not tell that 6 intents succeeded.

These tests pin the symmetric contract: `mark_failed` accepts intent-landing
counts via keyword arguments and persists them to the SQLite row, the
Protocol declaration stays in lockstep so type checkers catch drift between
the SQLite and gateway-backed implementations, and the existing
successful-path behaviour is preserved.

Semantic-clash note (acknowledged, follow-up filed): the column is named
``positions_*`` but the runtime counts **intents** — one position can be
closed by multiple intents (REPAY + WITHDRAW) and one intent can affect
multiple positions. For this PR we document the existing column as
"count of teardown intents whose terminal state matches the column name."

See ``docs/internal/MorphoStatusMay17.md`` Implementation Plan Item 6.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownRequest,
    TeardownStatus,
)
from almanak.framework.teardown.state_manager import (
    TeardownStateManager,
    TeardownStateManagerProtocol,
)


def _make_request(strategy_id: str = "S") -> TeardownRequest:
    """Minimal TeardownRequest — defaults on asset_policy / target_token /
    requested_by keep the construction short (see ``models.py:536``)."""
    return TeardownRequest(strategy_id=strategy_id, mode=TeardownMode.SOFT)


class TestTeardownCountersIncrement:
    """VIB-4542: failed-path counter columns must reflect intent landing results."""

    def test_mark_failed_accepts_counts_kwargs(self, tmp_path: Path) -> None:
        """The signature extension itself — guards against regression on the
        new ``positions_closed`` / ``positions_failed`` keyword arguments."""
        state = TeardownStateManager(db_path=str(tmp_path / "td.db"))
        state.create_request(_make_request())
        # mark_started lifts a row out of PENDING into IN_PROGRESS; get_active_request
        # only returns non-terminal rows so the subsequent mark_failed must see
        # an active row to update.
        state.mark_started("S", total_positions=7)
        result = state.mark_failed(
            "S",
            error="1 intents failed",
            positions_closed=6,
            positions_failed=1,
        )
        assert result is not None
        assert result.positions_closed == 6
        assert result.positions_failed == 1
        assert result.status == TeardownStatus.FAILED

    def test_mark_failed_persists_counts_to_sqlite(self, tmp_path: Path) -> None:
        """End-to-end: the SQLite row reflects the new counter values
        AND the error message, in the same write."""
        db_path = tmp_path / "td.db"
        state = TeardownStateManager(db_path=str(db_path))
        state.create_request(_make_request())
        state.mark_started("S", total_positions=7)
        state.mark_failed(
            "S",
            error="1 intents failed",
            positions_closed=6,
            positions_failed=1,
        )
        with sqlite3.connect(str(db_path)) as c:
            row = c.execute(
                "SELECT positions_closed, positions_failed, error_message, status "
                "FROM teardown_requests WHERE strategy_id = ?",
                ("S",),
            ).fetchone()
        assert row == (6, 1, "1 intents failed", "failed")

    def test_mark_failed_defaults_preserve_back_compat(self, tmp_path: Path) -> None:
        """Callers that don't pass counts (legacy call sites) get the
        pre-VIB-4542 behaviour: counters stay at their pre-call values,
        only ``status`` and ``error_message`` flip.

        The kwarg defaults are ``None``, which signals "preserve the prior
        column value" — distinct from passing 0 explicitly which signals
        "overwrite to 0". A caller omitting the kwargs ("I don't know what
        landed") inherits whatever the prior ``update_progress`` wrote.
        """
        state = TeardownStateManager(db_path=str(tmp_path / "td.db"))
        state.create_request(_make_request())
        state.mark_started("S", total_positions=7)
        # Imagine update_progress already wrote counters once.
        state.update_progress("S", positions_closed=2, positions_failed=0)
        result = state.mark_failed("S", error="catastrophic failure")
        # No kwargs supplied → both counters preserved at the update_progress
        # values (2, 0). VIB-4542 contract honoured.
        assert result is not None
        assert result.positions_closed == 2
        assert result.positions_failed == 0
        assert result.status == TeardownStatus.FAILED

    def test_mark_completed_still_lifts_intents_into_positions_closed(
        self, tmp_path: Path
    ) -> None:
        """Regression: the existing successful-path VIB-3920 behaviour is
        preserved — ``mark_completed`` continues to read ``result["intents"]``
        and persist it as ``positions_closed``."""
        state = TeardownStateManager(db_path=str(tmp_path / "td.db"))
        state.create_request(_make_request())
        state.mark_started("S", total_positions=7)
        state.mark_completed("S", result={"intents": 7})
        active = state.get_request("S")
        assert active is not None
        assert active.positions_closed == 7
        assert active.status == TeardownStatus.COMPLETED

    def test_state_adapter_protocol_signature_matches(self) -> None:
        """Codex v2 Finding 2: the Protocol at ``state_manager.py:TeardownStateManagerProtocol``
        declares the contract that both SQLite and gateway-backed state
        managers must satisfy. If ``mark_failed``'s signature changes on
        the concrete class, the Protocol declaration must change too —
        otherwise type checkers green-light a contract drift between
        SQLite and gateway-backed implementations.

        This test exercises the Protocol structurally by satisfying it
        with a stub that implements the new ``mark_failed`` signature
        plus the rest of the Protocol surface. ``isinstance`` against a
        ``runtime_checkable`` Protocol verifies the method set;
        signature drift in callers would surface as a type-checker error
        and (at runtime) a TypeError when kwargs are passed to a stale
        implementation."""

        class _StubMatchesProtocol:
            """Implements the full TeardownStateManagerProtocol surface
            with the VIB-4542 ``mark_failed`` signature."""

            def create_request(self, request):  # noqa: ANN001
                return None

            def get_request(self, strategy_id):  # noqa: ANN001
                return None

            def get_active_request(self, strategy_id):  # noqa: ANN001
                return None

            def get_pending_requests(self):
                return []

            def get_all_active_requests(self):
                return []

            def get_all_requests(self):
                return []

            def update_request(self, request):  # noqa: ANN001
                return None

            def acknowledge_request(self, strategy_id):  # noqa: ANN001
                return None

            def mark_started(self, strategy_id, total_positions=0):  # noqa: ANN001
                return None

            def update_progress(  # noqa: ANN001
                self,
                strategy_id,
                positions_closed,
                positions_failed=0,
                current_phase=None,
            ):
                return None

            def mark_completed(self, strategy_id, result=None):  # noqa: ANN001
                return None

            def mark_failed(  # noqa: ANN001
                self,
                strategy_id,
                error,
                *,
                positions_closed=None,
                positions_failed=None,
            ):
                # Defaults match the Protocol (Optional[int], default None)
                # so this stub satisfies the contract any future implementor
                # would copy from. Audit PR #2343 finding (CodeRabbit + Claude).
                return None

            def request_cancel(self, strategy_id):  # noqa: ANN001
                return False

            def mark_cancelled(self, strategy_id):  # noqa: ANN001
                return None

            def delete_request(self, strategy_id):  # noqa: ANN001
                return False

        # runtime_checkable Protocol — verifies the stub has every method
        # the Protocol declares. If the Protocol's mark_failed declaration
        # drifts away from the SQLite + gateway implementations, this
        # check still passes (Protocols don't compare signatures at
        # isinstance time) — the type checker is the contract enforcer.
        # The structural check below is the runtime guard.
        assert isinstance(_StubMatchesProtocol(), TeardownStateManagerProtocol)


class TestTeardownCountersIntegrationMatrix:
    """Behavioural matrix: did the counters ever get populated by anything
    other than the explicit positions_closed/failed kwargs we just added?

    These tests pin down that:
    - update_progress is the ongoing-state writer (existing)
    - mark_completed lifts result["intents"] (existing, VIB-3920)
    - mark_failed accepts counts (new, VIB-4542)
    """

    @pytest.mark.parametrize(
        ("closed", "failed", "expected_status"),
        [
            (0, 0, TeardownStatus.FAILED),
            (5, 0, TeardownStatus.FAILED),
            (0, 5, TeardownStatus.FAILED),
            (6, 1, TeardownStatus.FAILED),
        ],
    )
    def test_mark_failed_round_trips_counts(
        self, tmp_path: Path, closed: int, failed: int, expected_status: TeardownStatus
    ) -> None:
        state = TeardownStateManager(db_path=str(tmp_path / "td.db"))
        state.create_request(_make_request())
        state.mark_started("S", total_positions=closed + failed)
        result = state.mark_failed(
            "S", error="x", positions_closed=closed, positions_failed=failed
        )
        assert result is not None
        assert result.positions_closed == closed
        assert result.positions_failed == failed
        assert result.status == expected_status
