"""Gateway round-trip coverage for VIB-4091 / 4093 / 4095 / 4097 snapshot
identity fields.

CodeRabbit (PR #2162) flagged that ``SaveSnapshotRequest`` and
``SnapshotData`` gained ``deployment_id`` / ``cycle_id`` /
``execution_mode`` but the gateway test suite did not exercise the
round-trip on either the PG path (UPSERT preserve / backfill) or the
SQLite path (rebuild → SQLiteStore → mapper → SnapshotData). Per
``CLAUDE.md``: "almanak/gateway/** … Changes require gateway unit tests
(tests/gateway/)."

Scope of this file: just the identity-field plumbing. The legacy
SavePortfolioSnapshot / GetLatestSnapshot / GetSnapshotsSince behaviour
is already covered by ``test_state_service_characterization.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer
from tests.gateway.grpc_harness import make_grpc_context

# ──────────────────────────────────────────────────────────────────────────────
# Fixtures (mirror test_state_service_characterization.py shape)
# ──────────────────────────────────────────────────────────────────────────────


def _make_settings(database_url: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(database_url=database_url, standalone=False)


@pytest.fixture(autouse=True)
def _isolate_agent_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip AGENT_ID so resolve_agent_id() passes through verbatim."""
    monkeypatch.delenv("AGENT_ID", raising=False)


@pytest.fixture
def context() -> MagicMock:
    return make_grpc_context()


@pytest.fixture
def warm_backend() -> AsyncMock:
    wb = AsyncMock()
    wb.save_portfolio_snapshot.return_value = 99
    wb.get_latest_snapshot.return_value = None
    wb.get_snapshots_since.return_value = []
    return wb


@pytest.fixture
def state_manager(warm_backend: AsyncMock) -> AsyncMock:
    sm = AsyncMock()
    sm.warm_backend = warm_backend
    return sm


@pytest.fixture
def sqlite_service(state_manager: AsyncMock) -> StateServiceServicer:
    """SQLite mode (no _snapshot_pool)."""
    svc = StateServiceServicer(_make_settings())
    svc._state_manager = state_manager
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = None
    return svc


@pytest.fixture
def pg_service(state_manager: AsyncMock) -> StateServiceServicer:
    """PostgreSQL mode (_snapshot_pool present, helpers patchable)."""
    svc = StateServiceServicer(_make_settings(database_url="postgres://x/y"))
    svc._state_manager = state_manager
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = MagicMock()
    return svc


def _make_save_request(
    *,
    deployment_id: str = "Strat:abc",
    cycle_id: str = "cycle-001",
    execution_mode: str = "live",
) -> gateway_pb2.SaveSnapshotRequest:
    return gateway_pb2.SaveSnapshotRequest(
        strategy_id="Strat:abc",
        timestamp=1_725_000_000,
        iteration_number=1,
        total_value_usd="1000.00",
        available_cash_usd="500.00",
        value_confidence="HIGH",
        positions_json=b"[]",
        chain="arbitrum",
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
    )


# ──────────────────────────────────────────────────────────────────────────────
# PG path — UPSERT identity preservation
# ──────────────────────────────────────────────────────────────────────────────


class TestPgSnapshotIdentityWrite:
    """Exercise the asymmetric ``ON CONFLICT DO UPDATE ... CASE WHEN
    existing IS NULL OR existing = ''`` pattern at state_service.py:602-654."""

    @pytest.mark.asyncio
    async def test_pg_save_passes_identity_fields_into_insert(self, pg_service, context) -> None:
        async def _fake_fetchrow(query, *args):
            return {"id": 42}

        with patch.object(
            pg_service,
            "_snapshot_fetchrow",
            new=AsyncMock(side_effect=_fake_fetchrow),
        ) as fake:
            response = await pg_service.SavePortfolioSnapshot(
                _make_save_request(
                    deployment_id="dep-1",
                    cycle_id="cycle-7",
                    execution_mode="live",
                ),
                context,
            )
        assert response.success is True
        assert response.snapshot_id == 42

        query, *args = fake.call_args.args
        # Identity columns must appear in the INSERT column list.
        assert "deployment_id" in query
        assert "cycle_id" in query
        assert "execution_mode" in query
        # And the asymmetric preserve clause must be present (otherwise a
        # second unstamped write would blank a stamped row — May 7 incident
        # class on the SQLite side, ported here for the PG path).
        assert "WHEN portfolio_snapshots.deployment_id IS NULL" in query
        assert "WHEN portfolio_snapshots.cycle_id IS NULL" in query
        assert "WHEN portfolio_snapshots.execution_mode IS NULL" in query
        # Values arrive as the LAST three positional args, in declared order.
        assert tuple(args[-3:]) == ("dep-1", "cycle-7", "live")

    @pytest.mark.asyncio
    async def test_pg_save_with_omitted_identity_falls_back_to_empty(self, pg_service, context) -> None:
        """Legacy clients that don't set the new fields must still succeed;
        the wire default for proto3 string fields is ``""`` and the writer
        passes that through verbatim. Hosted PG's CASE preserves any
        previously-written value, so the missing-identity write is a safe
        no-op on a stamped row."""

        async def _fake_fetchrow(query, *args):
            return {"id": 7}

        request = gateway_pb2.SaveSnapshotRequest(
            strategy_id="Strat:abc",
            timestamp=1_725_000_001,
            total_value_usd="0",
            available_cash_usd="0",
            value_confidence="HIGH",
            positions_json=b"[]",
            chain="arbitrum",
            # deployment_id/cycle_id/execution_mode left unset — proto3 default ""
        )
        with patch.object(
            pg_service,
            "_snapshot_fetchrow",
            new=AsyncMock(side_effect=_fake_fetchrow),
        ) as fake:
            response = await pg_service.SavePortfolioSnapshot(request, context)
        assert response.success is True
        args = fake.call_args.args[1:]
        assert tuple(args[-3:]) == ("", "", "")


# ──────────────────────────────────────────────────────────────────────────────
# SQLite path — rebuilt PortfolioSnapshot carries identity into the writer
# ──────────────────────────────────────────────────────────────────────────────


class TestSqliteSnapshotIdentityWrite:
    @pytest.mark.asyncio
    async def test_sqlite_save_rebuilds_snapshot_with_identity(self, sqlite_service, warm_backend, context) -> None:
        """The SQLite path rebuilds a ``PortfolioSnapshot`` from the wire
        request and hands it to the warm backend. The three identity fields
        must land on that rebuilt object so the SQLite writer (VIB-4096)
        can persist them — without this, hosted-vs-local would diverge."""
        await sqlite_service.SavePortfolioSnapshot(
            _make_save_request(
                deployment_id="dep-2",
                cycle_id="cycle-9",
                execution_mode="paper",
            ),
            context,
        )
        warm_backend.save_portfolio_snapshot.assert_awaited_once()
        snapshot = warm_backend.save_portfolio_snapshot.call_args.args[0]
        assert isinstance(snapshot, PortfolioSnapshot)
        assert snapshot.deployment_id == "dep-2"
        assert snapshot.cycle_id == "cycle-9"
        assert snapshot.execution_mode == "paper"

    @pytest.mark.asyncio
    async def test_sqlite_save_omitted_identity_yields_empty_strings(
        self, sqlite_service, warm_backend, context
    ) -> None:
        request = gateway_pb2.SaveSnapshotRequest(
            strategy_id="Strat:abc",
            timestamp=1_725_000_002,
            total_value_usd="0",
            available_cash_usd="0",
            value_confidence="HIGH",
            positions_json=b"[]",
            chain="arbitrum",
        )
        await sqlite_service.SavePortfolioSnapshot(request, context)
        snapshot = warm_backend.save_portfolio_snapshot.call_args.args[0]
        assert snapshot.deployment_id == ""
        assert snapshot.cycle_id == ""
        assert snapshot.execution_mode == ""


# ──────────────────────────────────────────────────────────────────────────────
# Read path — SnapshotData carries identity back to the SDK
# ──────────────────────────────────────────────────────────────────────────────


class TestSnapshotIdentityReadMapping:
    @pytest.mark.asyncio
    async def test_get_latest_includes_identity_fields(self, sqlite_service, warm_backend, context) -> None:
        snap = PortfolioSnapshot(
            timestamp=datetime(2026, 5, 7, 12, 0, tzinfo=UTC),
            strategy_id="Strat:abc",
            total_value_usd=Decimal("1234.56"),
            available_cash_usd=Decimal("500"),
            value_confidence=ValueConfidence.HIGH,
            chain="arbitrum",
            iteration_number=7,
            deployment_id="Strat:abc",
            cycle_id="cycle-read-001",
            execution_mode="live",
        )
        warm_backend.get_latest_snapshot.return_value = snap
        request = gateway_pb2.GetLatestSnapshotRequest(strategy_id="Strat:abc")
        response = await sqlite_service.GetLatestSnapshot(request, context)
        assert response.found is True
        assert response.deployment_id == "Strat:abc"
        assert response.cycle_id == "cycle-read-001"
        assert response.execution_mode == "live"

    @pytest.mark.asyncio
    async def test_get_latest_legacy_snapshot_emits_empty_identity(self, sqlite_service, warm_backend, context) -> None:
        """Snapshots persisted before VIB-4092 / 4096 don't carry identity;
        the proto must surface ``""`` rather than crash, so the dashboard
        can render legacy rows with a missing-identity badge."""
        snap = PortfolioSnapshot(
            timestamp=datetime(2026, 5, 7, 12, 0, tzinfo=UTC),
            strategy_id="Strat:abc",
            total_value_usd=Decimal("1234.56"),
            available_cash_usd=Decimal("500"),
            value_confidence=ValueConfidence.HIGH,
            chain="arbitrum",
            iteration_number=7,
            # No deployment_id/cycle_id/execution_mode — defaults to ""
        )
        warm_backend.get_latest_snapshot.return_value = snap
        request = gateway_pb2.GetLatestSnapshotRequest(strategy_id="Strat:abc")
        response = await sqlite_service.GetLatestSnapshot(request, context)
        assert response.found is True
        assert response.deployment_id == ""
        assert response.cycle_id == ""
        assert response.execution_mode == ""

    @pytest.mark.asyncio
    async def test_get_snapshots_since_passes_identity_for_each_row(
        self, sqlite_service, warm_backend, context
    ) -> None:
        snaps = [
            PortfolioSnapshot(
                timestamp=datetime(2026, 5, 7, 12, i, tzinfo=UTC),
                strategy_id="Strat:abc",
                total_value_usd=Decimal("100"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.HIGH,
                chain="arbitrum",
                iteration_number=i,
                deployment_id="Strat:abc",
                cycle_id=f"cycle-{i:03d}",
                execution_mode="live",
            )
            for i in range(3)
        ]
        warm_backend.get_snapshots_since.return_value = snaps
        request = gateway_pb2.GetSnapshotsSinceRequest(strategy_id="Strat:abc", since=0, limit=10)
        response = await sqlite_service.GetSnapshotsSince(request, context)
        assert len(response.snapshots) == 3
        for i, wire in enumerate(response.snapshots):
            assert wire.deployment_id == "Strat:abc"
            assert wire.cycle_id == f"cycle-{i:03d}"
            assert wire.execution_mode == "live"
