"""Tests for TeardownStateAdapter — SQLite persistence and approval channel.

Covers:
- Env var ALMANAK_STATE_DB path resolution
- TypeError on bad db_path type
- TeardownState round-trip (save/get/delete)
- Compound-PK approval requests (teardown_id, level) — levels don't clobber
- strategy_id-based lookup for API callers
- Schema migration from pre-release single-PK layout
- Concurrent process simulation (runner poll + API writer on the same file)
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.models import (
    EscalationLevel,
    TeardownMode,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.state_manager import TeardownStateAdapter, TeardownStateManager


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    return tmp_path / "state.db"


@pytest.fixture
def adapter(tmp_db_path: Path) -> TeardownStateAdapter:
    return TeardownStateAdapter(db_path=tmp_db_path)


def _make_teardown_state(strategy_id: str = "strat_1", teardown_id: str = "td_1") -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id=teardown_id,
        strategy_id=strategy_id,
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=3,
        completed_intents=1,
        current_intent_index=1,
        started_at=now,
        updated_at=now,
        completed_at=None,
        pending_intents_json="[]",
        intent_results=[],
        cancel_window_until=None,
        config_json="{}",
    )


# -----------------------------------------------------------------------------
# Path resolution
# -----------------------------------------------------------------------------


class TestPathResolution:
    def test_env_var_overrides_default(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """ALMANAK_STATE_DB takes precedence over the cwd default."""
        env_path = tmp_path / "env-selected.db"
        monkeypatch.setenv("ALMANAK_STATE_DB", str(env_path))
        monkeypatch.delenv("ALMANAK_TEARDOWN_STATE_DB", raising=False)

        resolved = TeardownStateManager._resolve_db_path(None)

        assert resolved == env_path

    def test_explicit_path_beats_env_var(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """An explicit argument wins over the env var."""
        env_path = tmp_path / "env.db"
        explicit = tmp_path / "explicit.db"
        monkeypatch.setenv("ALMANAK_STATE_DB", str(env_path))

        resolved = TeardownStateManager._resolve_db_path(explicit)

        assert resolved == explicit

    def test_hard_fails_when_no_strategy_folder_resolves(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """VIB-3835: the teardown DB resolver no longer falls through to the
        per-user utility default. Strategy-scoped operations must resolve to
        a real strategy folder; silently writing to ``~/.local/share/almanak/
        utility/almanak_state.db`` was the May 1 mainnet teardown failure
        mode (the runner polls the strategy-folder DB, the CLI was writing
        to the utility DB, the request was never seen).
        """
        from almanak.framework.local_paths import LocalPathError

        monkeypatch.delenv("AGENT_ID", raising=False)
        monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        with pytest.raises(LocalPathError, match="no strategy folder resolved"):
            TeardownStateManager._resolve_db_path(None)


# -----------------------------------------------------------------------------
# TypeError on bad db_path
# -----------------------------------------------------------------------------


class TestBadDbPath:
    def test_mock_db_path_raises(self) -> None:
        """A MagicMock (or other non-path) must fail loudly instead of silently
        falling back to CWD. Cross-process paths are too important for that."""
        with pytest.raises(TypeError, match="TeardownStateAdapter db_path"):
            TeardownStateAdapter(db_path=MagicMock())  # type: ignore[arg-type]

    def test_int_db_path_raises(self) -> None:
        with pytest.raises(TypeError):
            TeardownStateAdapter(db_path=123)  # type: ignore[arg-type]


# -----------------------------------------------------------------------------
# TeardownState round-trip
# -----------------------------------------------------------------------------


class TestTeardownStateRoundTrip:
    @pytest.mark.asyncio
    async def test_save_then_get_preserves_fields(self, adapter: TeardownStateAdapter) -> None:
        state = _make_teardown_state()

        await adapter.save_teardown_state(state)
        loaded = await adapter.get_teardown_state(state.strategy_id)

        assert loaded is not None
        assert loaded.teardown_id == state.teardown_id
        assert loaded.strategy_id == state.strategy_id
        assert loaded.status == TeardownStatus.EXECUTING
        assert loaded.total_intents == 3
        assert loaded.completed_intents == 1

    @pytest.mark.asyncio
    async def test_get_missing_strategy_returns_none(self, adapter: TeardownStateAdapter) -> None:
        assert await adapter.get_teardown_state("nonexistent") is None

    @pytest.mark.asyncio
    async def test_save_twice_is_idempotent_latest_wins(self, adapter: TeardownStateAdapter) -> None:
        state = _make_teardown_state()
        await adapter.save_teardown_state(state)

        updated = TeardownState(
            teardown_id=state.teardown_id,
            strategy_id=state.strategy_id,
            mode=state.mode,
            status=TeardownStatus.COMPLETED,
            total_intents=3,
            completed_intents=3,
            current_intent_index=3,
            started_at=state.started_at,
            updated_at=state.updated_at + timedelta(seconds=5),
            completed_at=state.updated_at + timedelta(seconds=5),
            pending_intents_json="[]",
            intent_results=[],
            cancel_window_until=None,
            config_json="{}",
        )
        await adapter.save_teardown_state(updated)

        loaded = await adapter.get_teardown_state(state.strategy_id)

        assert loaded is not None
        assert loaded.status == TeardownStatus.COMPLETED
        assert loaded.completed_intents == 3

    @pytest.mark.asyncio
    async def test_delete_removes_row(self, adapter: TeardownStateAdapter) -> None:
        state = _make_teardown_state()
        await adapter.save_teardown_state(state)

        await adapter.delete_teardown_state(state.teardown_id)

        assert await adapter.get_teardown_state(state.strategy_id) is None

    @pytest.mark.asyncio
    async def test_corrupted_intent_results_json_falls_back_to_empty(
        self, adapter: TeardownStateAdapter, tmp_db_path: Path
    ) -> None:
        """A corrupted intent_results_json blob must not prevent resumption —
        the runner should still be able to load the state and proceed.
        """
        state = _make_teardown_state()
        await adapter.save_teardown_state(state)

        # Corrupt the row directly at the SQLite layer to simulate a disk
        # or migration mishap that left non-JSON bytes in the column.
        with sqlite3.connect(tmp_db_path) as conn:
            conn.execute(
                "UPDATE teardown_execution_state SET intent_results_json = ? WHERE teardown_id = ?",
                ("not a json blob {", state.teardown_id),
            )
            conn.commit()

        loaded = await adapter.get_teardown_state(state.strategy_id)

        assert loaded is not None
        assert loaded.intent_results == []

    @pytest.mark.asyncio
    async def test_non_list_intent_results_json_falls_back_to_empty(
        self, adapter: TeardownStateAdapter, tmp_db_path: Path
    ) -> None:
        """If intent_results_json is valid JSON but not a list (e.g., a dict),
        fall back to an empty list rather than carrying a wrong-shape value."""
        state = _make_teardown_state()
        await adapter.save_teardown_state(state)

        with sqlite3.connect(tmp_db_path) as conn:
            conn.execute(
                "UPDATE teardown_execution_state SET intent_results_json = ? WHERE teardown_id = ?",
                ('{"oops": "this is a dict"}', state.teardown_id),
            )
            conn.commit()

        loaded = await adapter.get_teardown_state(state.strategy_id)

        assert loaded is not None
        assert loaded.intent_results == []


# -----------------------------------------------------------------------------
# Compound-PK approval requests
# -----------------------------------------------------------------------------


class TestApprovalCompoundKey:
    def test_two_levels_create_two_rows(self, adapter: TeardownStateAdapter) -> None:
        """Each (teardown_id, level) is its own row."""
        expires = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json=json.dumps({"slippage": "0.05"}),
            expires_at=expires,
        )
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_4,
            request_json=json.dumps({"slippage": "0.08"}),
            expires_at=expires,
        )

        # Both rows should be pending and retrievable independently.
        assert adapter.get_approval_response("td_1", EscalationLevel.LEVEL_3) is None
        assert adapter.get_approval_response("td_1", EscalationLevel.LEVEL_4) is None

    def test_response_for_one_level_does_not_satisfy_another(
        self, adapter: TeardownStateAdapter
    ) -> None:
        """A response written for level 3 must not be returned when polling for level 4.

        This is the core fix for the pre-release bug where INSERT OR REPLACE on
        a single-column PK let a level-3 response silently satisfy the level-4
        poll, turning "approve the 5% level" into "approve the 8% level".
        """
        expires = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json="{}",
            expires_at=expires,
        )
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_4,
            request_json="{}",
            expires_at=expires,
        )

        adapter.write_approval_response(
            teardown_id="td_1",
            level=EscalationLevel.LEVEL_3,
            response_json=json.dumps({"approved": True, "action": "approve"}),
        )

        l3 = adapter.get_approval_response("td_1", EscalationLevel.LEVEL_3)
        l4 = adapter.get_approval_response("td_1", EscalationLevel.LEVEL_4)

        assert l3 is not None
        assert l4 is None  # the critical assertion

    def test_write_response_returns_false_for_unknown_key(self, adapter: TeardownStateAdapter) -> None:
        ok = adapter.write_approval_response(
            teardown_id="td_missing",
            level=EscalationLevel.LEVEL_3,
            response_json="{}",
        )
        assert ok is False


# -----------------------------------------------------------------------------
# strategy_id-based API convenience methods
# -----------------------------------------------------------------------------


class TestByStrategyHelpers:
    def test_get_latest_pending_returns_oldest_unresponded(
        self, adapter: TeardownStateAdapter
    ) -> None:
        """Operators respond to the alert that was sent — oldest pending first."""
        now = datetime.now(UTC)
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json="{}",
            expires_at=(now + timedelta(minutes=30)).isoformat(),
        )
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_4,
            request_json="{}",
            expires_at=(now + timedelta(minutes=30)).isoformat(),
        )

        pending = adapter.get_latest_pending_approval("strat_1")

        assert pending is not None
        assert pending["level"] == EscalationLevel.LEVEL_3.value

    def test_get_latest_pending_skips_responded(self, adapter: TeardownStateAdapter) -> None:
        now = datetime.now(UTC)
        expires = (now + timedelta(minutes=30)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json="{}",
            expires_at=expires,
        )
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_4,
            request_json="{}",
            expires_at=expires,
        )
        adapter.write_approval_response(
            teardown_id="td_1",
            level=EscalationLevel.LEVEL_3,
            response_json="{}",
        )

        pending = adapter.get_latest_pending_approval("strat_1")

        assert pending is not None
        assert pending["level"] == EscalationLevel.LEVEL_4.value

    def test_get_latest_pending_none_when_all_responded(self, adapter: TeardownStateAdapter) -> None:
        expires = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json="{}",
            expires_at=expires,
        )
        adapter.write_approval_response(
            teardown_id="td_1",
            level=EscalationLevel.LEVEL_3,
            response_json="{}",
        )

        assert adapter.get_latest_pending_approval("strat_1") is None

    def test_write_response_by_strategy_writes_to_oldest(
        self, adapter: TeardownStateAdapter
    ) -> None:
        expires = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json="{}",
            expires_at=expires,
        )

        ok = adapter.write_approval_response_by_strategy(
            strategy_id="strat_1",
            response_json=json.dumps({"approved": True, "action": "approve"}),
        )

        assert ok is True
        body = adapter.get_approval_response("td_1", EscalationLevel.LEVEL_3)
        assert body is not None
        assert json.loads(body) == {"approved": True, "action": "approve"}

    def test_write_response_by_strategy_false_when_nothing_pending(
        self, adapter: TeardownStateAdapter
    ) -> None:
        assert (
            adapter.write_approval_response_by_strategy(
                strategy_id="never_existed",
                response_json="{}",
            )
            is False
        )


# -----------------------------------------------------------------------------
# Cross-process simulation: API writer vs runner poller (same SQLite file)
# -----------------------------------------------------------------------------


class TestCrossProcessCoordination:
    def test_api_write_is_visible_to_runner_adapter(self, tmp_db_path: Path) -> None:
        """Two adapters on the same file must see each other's writes.

        Simulates the production flow where the API process writes an approval
        response and the runner process polls the same file for it.
        """
        api_adapter = TeardownStateAdapter(db_path=tmp_db_path)
        runner_adapter = TeardownStateAdapter(db_path=tmp_db_path)

        expires = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
        runner_adapter.create_approval_request(
            teardown_id="td_1",
            strategy_id="strat_1",
            level=EscalationLevel.LEVEL_3,
            request_json="{}",
            expires_at=expires,
        )

        # API writes the operator's decision.
        ok = api_adapter.write_approval_response_by_strategy(
            strategy_id="strat_1",
            response_json=json.dumps({"approved": True, "action": "approve"}),
        )

        # Runner polls and sees it.
        body = runner_adapter.get_approval_response("td_1", EscalationLevel.LEVEL_3)

        assert ok is True
        assert body is not None
        assert json.loads(body)["action"] == "approve"


# -----------------------------------------------------------------------------
# Schema migration from pre-release single-PK layout
# -----------------------------------------------------------------------------


class TestSchemaMigration:
    def test_legacy_schema_is_migrated_to_compound_pk(self, tmp_db_path: Path) -> None:
        """If the DB was populated by an older build of the adapter with a
        single-column PK on teardown_approvals, we rebuild the table rather
        than carrying stale rows that can't express a (teardown_id, level) key.
        """
        # Create the legacy schema directly.
        with sqlite3.connect(tmp_db_path) as conn:
            conn.execute(
                """
                CREATE TABLE teardown_approvals (
                    teardown_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT,
                    created_at TEXT NOT NULL,
                    responded_at TEXT,
                    expires_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """INSERT INTO teardown_approvals
                   (teardown_id, strategy_id, request_json, created_at, expires_at)
                   VALUES ('td_legacy', 'strat_legacy', '{}', ?, ?)""",
                (datetime.now(UTC).isoformat(), datetime.now(UTC).isoformat()),
            )
            conn.commit()

        # Constructing the adapter should migrate.
        adapter = TeardownStateAdapter(db_path=tmp_db_path)

        # Legacy row is gone (pre-release migration).
        with sqlite3.connect(tmp_db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("PRAGMA table_info(teardown_approvals)")
            columns = {row[1] for row in cursor.fetchall()}
            rows = conn.execute("SELECT COUNT(*) as c FROM teardown_approvals").fetchone()

        assert "level" in columns
        assert rows["c"] == 0

        # And the new compound PK works.
        expires = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
        adapter.create_approval_request(
            teardown_id="td_new",
            strategy_id="strat_new",
            level=EscalationLevel.LEVEL_3,
            request_json="{}",
            expires_at=expires,
        )
        pending = adapter.get_latest_pending_approval("strat_new")
        assert pending is not None
        assert pending["teardown_id"] == "td_new"


# -----------------------------------------------------------------------------
# Async methods don't block the event loop
# -----------------------------------------------------------------------------


class TestAsyncOffThread:
    @pytest.mark.asyncio
    async def test_save_get_delete_do_not_block(self, adapter: TeardownStateAdapter) -> None:
        """Async protocol methods must go through asyncio.to_thread so SQLite
        calls don't block the event loop. Regression test for the audit
        finding about synchronous sqlite inside async methods."""
        state = _make_teardown_state()

        async def heartbeat() -> int:
            ticks = 0
            for _ in range(5):
                await asyncio.sleep(0)
                ticks += 1
            return ticks

        # If save_teardown_state blocked the loop, the heartbeat coroutine
        # wouldn't be able to make any progress until after save returns.
        ticks, _ = await asyncio.gather(
            heartbeat(),
            adapter.save_teardown_state(state),
        )
        assert ticks == 5

        loaded, _ = await asyncio.gather(
            adapter.get_teardown_state(state.strategy_id),
            heartbeat(),
        )
        assert loaded is not None

        await adapter.delete_teardown_state(state.teardown_id)
        assert await adapter.get_teardown_state(state.strategy_id) is None
